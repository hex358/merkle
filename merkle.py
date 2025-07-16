import hashlib
from collections import defaultdict
from typing import List, Dict, Optional, Tuple, Any

# Merkle-Mountain-Range (mmr) data structures
node_hashes: Dict[int, bytes] = {}                      # append-only dict of node hashes
node_hash_indexes: Dict[bytes, int] = defaultdict(list)  # map node-hash to its positions
internal_nodes_by_height: Dict[int, Dict[int, bytes]] = {}    # map level->(start_index->subtree-root)
peaks: List[Optional[bytes]] = []                   # current peak roots by height
peaks_start: List[Optional[int]] = []               # start index of each peak in node_hashes

# hashing function using blake2b for faster digests
def kief(data: bytes) -> bytes:
    return hashlib.blake2b(data, digest_size = 16).digest()

# add a new blob to the mmr
# - compute its hash
# - append to node_hashes and record its index
# - merge any existing peaks of the same height
# - record internal nodes for proof generation
# O(1) amortized, O(log n) worst-case


def add(blob: bytes) -> None:
    # hash the input blob
    h: bytes = kief(blob)
    idx = len(node_hashes)
    # append node hash and index lookup
    if h in node_hash_indexes: return

    node_hashes[idx] = h
    node_hash_indexes[h] = idx

    # initialize new peak at height 0
    level = 0
    root = h
    start = idx

    # ensure peaks arrays cover this level
    if not peaks:
        peaks.append(None)
        peaks_start.append(None)

    # merge peaks of equal size up the tree
    while peaks[level] is not None:
        left_root = peaks[level]
        left_start = peaks_start[level]

        # clear merged peak slot
        peaks[level] = None
        peaks_start[level] = None

        # choose merge order by earliest start index
        if left_start < start:
            left, right = left_root, root
            merged_start = left_start
        else:
            left, right = root, left_root
            merged_start = start

        # compute parent node hash
        parent = kief(left + right)

        # store internal node for proof access
        internal_nodes_by_height.setdefault(level+1, {})[merged_start] = parent

        # update root and start for next level
        root = parent
        start = merged_start
        level += 1
        # expand peaks arrays if needed
        while level >= len(peaks):
            peaks.append(None)
            peaks_start.append(None)

    # insert final merged peak
    peaks[level] = root
    peaks_start[level] = start


# compute the current global root of the forest
# - find highest non-empty peak
# - fold in remaining peaks from highest to lowest

def get_global_root() -> bytes:
    root: Optional[bytes] = None
    # locate highest peak
    for lvl in reversed(range(len(peaks))):
        if peaks[lvl] is not None:
            root = peaks[lvl]
            break
    # if no peaks, return empty
    if root is None:
        return b""

    # fold in all other peaks
    for lvl in reversed(range(len(peaks))):
        if peaks[lvl] is not None and peaks[lvl] is not root:
            root = kief(root + peaks[lvl])
    return root

# server-side inclusion proof generation
# - locate blob by hash
# - find which peak contains it
# - collect sibling hashes at each level
# - gather other peak roots for final folding

def server_check(blob: bytes) -> Dict[str, Any]:
    target_h = kief(blob)
    node_index = node_hash_indexes.get(target_h)
    # return failure if blob not found
    if node_index is None:
        return {"status": 0, "detail": "Blob not found"}

    # find the peak that covers this node
    tree_level: Optional[int] = None
    tree_start: Optional[int] = None
    for lvl in reversed(range(len(peaks))):
        st = peaks_start[lvl]
        if st is None:
            continue
        size = 1 << lvl
        if st <= node_index < st + size:
            tree_level = lvl
            tree_start = st
            break

    # failure if inconsistent state
    if tree_level is None:
        return {"status": 0, "detail": "Inconsistent state"}

    # build merkle proof for this node
    proof: List[Tuple[bytes, bool]] = []
    local_idx = node_index - tree_start
    for lvl in range(tree_level):
        block = 1 << lvl
        grp = local_idx >> lvl
        sib_grp = grp ^ 1
        sib_start = tree_start + sib_grp * block

        # choose node or internal node
        if lvl == 0:
            sib_hash = node_hashes[sib_start]
        else:
            sib_hash = internal_nodes_by_height[lvl][sib_start]

        was_left = sib_grp < grp
        proof.append((sib_hash, was_left))

    # collect roots of all other peaks
    other_roots: List[bytes] = []
    for lvl in reversed(range(len(peaks))):
        if lvl == tree_level:
            continue
        root = peaks[lvl]
        if root is not None:
            other_roots.append(root)

    # return proof bundle
    return {
        "status":      1,
        "blob":        blob,
        "proof":       proof,
        "other_roots": other_roots,
        "global_root": get_global_root(),
    }

# client-side inclusion proof verification
# - recompute node hash
# - climb proof to reconstruct tree root
# - fold in other peaks to get global root
# - compare against server's value

def client_check(bundle: Dict[str, Any]) -> bool:
    if bundle.get("status") != 1:
        return False

    blob = bundle["blob"]
    proof = bundle["proof"]
    other_roots = bundle["other_roots"]
    expected = bundle["global_root"]

    # recompute node hash
    h = kief(blob)
    # climb up using proof
    for sib, was_left in proof:
        if was_left:
            h = kief(sib + h)
        else:
            h = kief(h + sib)

    # fold in remaining peaks
    root = h
    for r in other_roots:
        root = kief(root + r)

    return root == expected

from time import perf_counter
import timeit

t = perf_counter()
for i in range(2**20-1):
    add(str(i).encode("utf-8"))

print(sum(len(node) for node in internal_nodes_by_height.values()))

#from random import randint
#print(timeit.timeit(stmt="add(('i'+str(randint(0,100))).encode('utf-8'))", number=100, globals=globals()))

t = perf_counter()
proof = server_check(b"5")
print(client_check(proof))
print(perf_counter() - t)
