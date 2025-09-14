import hashlib
import struct
from typing import List, Dict, Optional, Tuple, Any

from database.interface import (
	StoredDict, StoredList, StoredReference, BatchingConfig,
	encode_val, decode_val, Start, open_environment
)

Start(1024, False, 30000)


def _lvl_key(level: int) -> bytes:
	return (str(level)).encode("ascii")


def _start_key(start_index: int) -> bytes:
	return (str(start_index)).encode("ascii")


def _encode_ref_to(obj) -> bytes:
	# encode as type 'r' + name using encode_val, which expects a StoredReference-typed value
	ref = StoredReference(obj)     # bare instance is fine; we just need a .name for encode_val
	#ref.name = obj.name
	return encode_val(ref)

_pack_u32 = struct.Struct(">I").pack
_pack_u64 = struct.Struct(">Q").pack


def _list_get_opt_bytes(lst: StoredList, idx: int) -> Optional[bytes]:
	val = lst[idx]
	# peaks[] are always encode_val(...) so decode them
	return decode_val(val)


def _list_set_opt_bytes(lst: StoredList, idx: int, maybe_bytes: Optional[bytes]) -> None:
	lst[idx] = encode_val(maybe_bytes)


def _list_append_opt_bytes(lst: StoredList, maybe_bytes: Optional[bytes]) -> None:
	lst.append(encode_val(maybe_bytes))


def _dict_get_optional(sd: StoredDict, key: bytes) -> Optional[bytes]:
	try:
		return sd[key]
	except KeyError:
		return None


# ---------- hashing ----------

def kief(*args: Any) -> bytes:
	h = hashlib.blake2b(digest_size=16)
	for a in args:
		if isinstance(a, memoryview):
			a = bytes(a)
		# assume other args are bytes
		h.update(a)
	return h.digest()


def get_meta(name: bytes):
	try:
		return db_boosts.deserialize(StoredDict(name=b"__services")[name])
	except Exception as e:
		return None

def has_service(name: str):
	return name.encode() in StoredDict(name=b"__services")

import db_boosts
def set_service(name: str, meta: dict) -> bool:
	if has_service(name):
		map = StoredDict(name=b"__services")
		map[name.encode()] = db_boosts.serialize(meta)
		map.flush_buffer()
		return True
	else:
		return False


class MerkleService:
	def __init__(self, subenv_name: str, meta: dict = {}):
		if not has_service(subenv_name):
			map = StoredDict(name=b"__services")
			map[subenv_name.encode()] = db_boosts.serialize(meta)
			map.flush_buffer()
		env = open_environment(subenv_name, 1024, False, 2*22)
		self.env = env

		# general list of hashes
		self.node_hashes: StoredList = StoredList(name=b"node_hashes", batching_config=BatchingConfig(512, True, 16),
		                                          env=env)

		# hash(bytes) -> index(int)  (value encoded with encode_val)
		self.node_hash_indexes: StoredDict = StoredDict(name=b"node_hash_indexes", batching_config=BatchingConfig(512),
		                                          env=env)

		# level(bytes) -> StoredReference(child StoredDict)
		#   child StoredDict name: b"mmr_internal_nodes_lvl_" + pack(">I", level)
		#   inside each child: start_index(bytes) -> parent_hash(bytes)
		self.internal_nodes_by_height: StoredDict = StoredDict(name=b"internal_nodes_by_height", batching_config=BatchingConfig(512),
		                                          env=env)

		# peaks: list of Optional[bytes]     (each item encoded via encode_val; None -> b"n")
		self.peaks: StoredList = StoredList(name=b"peaks", batching_config=BatchingConfig(512),
		                                          env=env)

		# peaks_start: list of Optional[int] (each item encoded via encode_val; None -> b"n")
		self.peaks_start: StoredList = StoredList(name=b"peaks_start", batching_config=BatchingConfig(512),
		                                          env=env)

		self.to_flush = set([])

	INTERNAL_PREFIX = b"nlvl"

	def flush(self):
		for i in self.to_flush:
			i.flush_buffer()
		self.node_hashes.flush_buffer()
		self.node_hash_indexes.flush_buffer()
		self.internal_nodes_by_height.flush_buffer()
		self.peaks.flush_buffer()
		self.peaks_start.flush_buffer()

	def _iter_peaks_sorted(self) -> List[Tuple[int, bytes, int]]:
		"""
		Return [(start, root, level)] sorted by start (left -> right).
		"""
		out: List[Tuple[int, bytes, int]] = []
		for lvl in range(len(self.peaks)):
			root = _list_get_opt_bytes(self.peaks, lvl)
			if root is None:
				continue
			st = decode_val(self.peaks_start[lvl])
			if st is None:
				continue
			out.append((st, root, lvl))
		out.sort(key=lambda t: t[0])
		return out


	def _get_child_level_dict(self, level: int) -> StoredDict:
		"""
		Fetch or lazily create per-level StoredDict that keeps internal nodes for that level.
		Top dict value is a StoredReference encoded via encode_val('r' + name).
		"""
		k = _lvl_key(level)
		try:
			raw = self.internal_nodes_by_height[k]
			child = decode_val(raw)  # -> StoredObject (StoredDict)
			self.to_flush.add(child)
			return child  # already registered by interface
		except KeyError:
			# create, register, then store reference
			name = self.INTERNAL_PREFIX + k
			child = StoredDict(name=name, batching_config=BatchingConfig(512),
		                                          env=self.env)
			self.internal_nodes_by_height[k] = _encode_ref_to(child)
			self.to_flush.add(child)
			return child


	def _ensure_level_slot(self, level: int) -> None:
		# grow peaks/peaks_start to include 'level'
		while level >= len(self.peaks):
			_list_append_opt_bytes(self.peaks, None)
			_list_append_opt_bytes(self.peaks_start, None)



	def add(self, h: bytes) -> None:
		# hash and append to node_hashes
		#h: bytes = kief(blob)
		idx = len(self.node_hashes)
		self.node_hashes.append(h)

		# de-dup by hash => index
		if h in self.node_hash_indexes:
			return

		self.node_hashes[idx] = h
		self.node_hash_indexes[h] = encode_val(idx)

		# initialize new peak (level 0)
		level = 0
		root = h
		start = idx

		if len(self.peaks) == 0:
			_list_append_opt_bytes(self.peaks, None)
			_list_append_opt_bytes(self.peaks_start, None)

		# merge while same-height peak exists
		self._ensure_level_slot(level)
		while _list_get_opt_bytes(self.peaks, level) is not None:
			left_root = _list_get_opt_bytes(self.peaks, level)          # bytes
			left_start = decode_val(self.peaks_start[level])             # int|None -> int
			# clear slot
			_list_set_opt_bytes(self.peaks, level, None)
			_list_set_opt_bytes(self.peaks_start, level, None)

			# choose order by earliest start index
			if left_start < start:
				left, right = left_root, root
				merged_start = left_start
			else:
				left, right = root, left_root
				merged_start = start

			# parent hash
			parent = kief(left, right)

			# store internal node for next level (nested dict via StoredReference)
			next_lvl = level + 1
			child = self._get_child_level_dict(next_lvl)
			child[_start_key(merged_start)] = parent  # value is raw bytes

			# move up
			root = parent
			start = merged_start
			level = next_lvl
			self._ensure_level_slot(level)

		# final peak at 'level'
		_list_set_opt_bytes(self.peaks, level, root)
		self.peaks_start[level] = encode_val(start)


	def get_global_root(self) -> bytes:
		ps = self._iter_peaks_sorted()
		if not ps:
			return b""
		acc = ps[0][1]
		for _, r, _ in ps[1:]:
			acc = kief(acc, r)
		return acc


	def server_check(self, target_h: bytes) -> Dict[str, Any]:
		raw_idx = _dict_get_optional(self.node_hash_indexes, target_h)
		if raw_idx is None:
			return {"status": 0, "detail": "Blob not found"}
		node_index: int = decode_val(raw_idx)

		# locate the covering peak
		ps = self._iter_peaks_sorted()
		tree_level: Optional[int] = None
		tree_start: Optional[int] = None
		for st, r, lvl in ps:
			size = 1 << lvl
			if st <= node_index < st + size:
				tree_level = lvl
				tree_start = st
				break
		if tree_level is None:
			return {"status": 0, "detail": "Inconsistent state"}

		# build Merkle proof inside the peak
		proof: List[Tuple[bytes, bool]] = []
		local_idx = node_index - tree_start
		for lvl in range(tree_level):
			block = 1 << lvl
			grp = local_idx >> lvl
			sib_grp = grp ^ 1
			sib_start = tree_start + sib_grp * block

			if lvl == 0:
				sib_hash = self.node_hashes[sib_start]
			else:
				child = self._get_child_level_dict(lvl)
				sib_hash = child[_start_key(sib_start)]
			was_left = sib_grp < grp
			proof.append((sib_hash.hex(), was_left))

		# split peaks into left and right
		left_roots: List[bytes] = [r.hex() for st, r, lvl in ps if st < tree_start]
		right_roots: List[bytes] = [r.hex() for st, r, lvl in ps if st > tree_start]

		return {
			"status": 1,
			"h": target_h.hex(),
			"proof": proof,
			"left_roots": left_roots,
			"right_roots": right_roots,
			"global_root": self.get_global_root().hex(),
		}


def client_check(bundle: Dict[str, Any]) -> bool:
    if bundle.get("status") != 1:
        return False
    tokief = bytes.fromhex
    proof = bundle["proof"]
    expected = tokief(bundle["global_root"])

    # recompute peak root
    h = tokief(bundle["h"])
    for sib_hex, was_left in proof:
        sib = tokief(sib_hex)
        h = kief(sib, h) if was_left else kief(h, sib)

    # fold left peaks then right peaks
    left_roots  = bundle.get("left_roots", [])
    right_roots = bundle.get("right_roots", bundle.get("other_roots", []))

    if left_roots:
        acc = tokief(left_roots[0])
        for r in left_roots[1:]:
            acc = kief(acc, tokief(r))
        h = kief(acc, h)

    for r in right_roots:
        h = kief(h, tokief(r))

    return h == expected

# # ---------- quick bench ----------
from time import perf_counter

# if __name__ == "__main__":
# 	# t = perf_counter()
# 	svc = MerkleService("miuff")
# 	for i in range(5000):
# 		svc.add(kief(str(i).encode("ascii")))
# 	svc.flush()
# 	t = perf_counter()
# 	print(client_check(svc.server_check(kief(str(435).encode("ascii")))))
# 	print(perf_counter() - t)
