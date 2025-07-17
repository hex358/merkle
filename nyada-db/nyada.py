# Nyada 1.0.0

import lmdb
import struct
from threading import Thread
import os

if not os.path.isdir("db"):
    os.mkdir("db")

def open_environment(name: str,
                     size_mb: float,
                     lock_safe: bool = True,
                     max_variables: int = 1024):
    return lmdb.open(
    "db/" + name,
    max_dbs=max_variables,
    map_size=size_mb * 1024 ** 2,
    writemap=True,
    map_async=True,
    metasync=lock_safe,
    sync=lock_safe
)

env = open_environment("db", 1024, False) # 1 GiB

class StoredReference:
    references = {}

    @staticmethod
    def get_from_name(id: int):
        return StoredReference.references[id]

    def __init__(self, of: "StoredObject"):
        assert isinstance(of, StoredObject), f'"of" must be a StoredObject, not {type(of).__name__}'
        StoredReference.references[id(of)] = of
class Types:
    TYPE_STR = str
    TYPE_INT = int
    TYPE_BYTES = bytes
    TYPE_NONE = type(None)


def encode_val(value) -> bytes:
    match type(value):
        case Types.TYPE_STR:
            return b's' + value.encode('ascii')
        case Types.TYPE_INT:
            return b'i' + str(value).encode('ascii')
        case Types.TYPE_BYTES:
            return b'b' + value
        case Types.TYPE_NONE:
            return b'n'
        case _:
            raise TypeError(f"unsupported type: {type(value)}")

def decode_val(data: bytes):
    code, payload = data[0], data[1:]
    match code:
        case 115:   # ord('s')
            return payload.decode('ascii')
        case 105:   # ord('i')
            return int(payload)
        case 98:    # ord('b')
            return payload
        case 110:   # ord('n')
            return None
        case _:
            raise ValueError(f"unknown typecode: {chr(code)}")

from math import log2
class StoredObject:
    __slots__ = ("variant_typed", "flushing_thread", "name", "cache_on_set", "env")


    def _flush_buffer(self):
        pass

    def wait_for_flush(self):
        if self.flushing_thread is not None:
            self.flushing_thread.join()
            self.flushing_thread = None

    def is_flushing_buffer(self):
        return self.flushing_thread and self.flushing_thread.is_alive()

    def flush_buffer(self, threaded=False):
        if self.flushing_thread is not None:
            self.flushing_thread.join()
            self.flushing_thread = None
        if threaded:
            self.flushing_thread = Thread(target=self._flush_buffer)
            self.flushing_thread.start()
        else:
            self._flush_buffer()

    @staticmethod
    def get_struct_format(n):
        # U min/max for 1, 2, 4, 8 bytes
        struct_types = {0: ">B", 1: ">B", 2: ">H", 4: ">L", 8: ">Q"}
        keys = list(struct_types.keys())
        for i in range(1, len(struct_types)):
            if keys[i]-1 <= n <= keys[i]:
                return struct_types[keys[i]]
        raise OverflowError("number too large for 8 bytes unsigned")

    def __init__(self,
                 name="",              # db name
                 env=env,              # dbs personal env (if not set, will use global env)
                 cache_on_set=True,    # auto cache on __setitem__ calls at cost of perfomance
                 max_length=0,         # maximal element length (for batch writes)
                 batch_writes=0,       # will put multiple values under same key for fewer PUT requests
                 constant_length=False # if True, and batching is enabled, will significantly speed it up,
                                       # with an assumption that length of every byte string is same
                 ):
        assert name, "Name must be a non-empty string"
        self.env = env
        self.cache_on_set = cache_on_set
        self.do_batch_writes = bool(batch_writes)
        self.batch_size = batch_writes
        self.flushing_thread = None

        self.byte_length = (max_length.bit_length() + 7) // 8
        self.HEADER_SLOT_COUNT = batch_writes + 1 # N items + final end-offset
        self.HEADER_BYTE_COUNT = self.HEADER_SLOT_COUNT * self.byte_length  # 1 short = 2 bytes
        struct_type = StoredObject.get_struct_format(self.byte_length)
        self.struct_pack_into = struct.Struct(struct_type).pack_into # unsigned big-endian short
        self.struct_unpack_from = struct.Struct(struct_type).unpack_from

        self.stat = env.open_db((name + "__stat").encode("ascii"), create=True)
        self.map_stat()

class StoredList(StoredObject):
    # LMDB-backed list with in-memory buffer and cache.
    # Append-only.

    int_pack = lambda index: struct.pack(">Q", index)

    def __init__(self, buffer_size: int = 0, **kwargs):
        super().__init__(**kwargs)
        name = kwargs["name"]
        # initialize lmdb db, buffer and cache
        self._db = env.open_db(name.encode("ascii"), create=True)
        self.stat = env.open_db((name + "__stat").encode("ascii"), create=True)

        with self.env.begin(db=self.stat) as txn:
            raw = txn.get(StoredList.LENGTH_KEY)
        if raw is None:
            self._persisted_len = 0
        else:
            self._persisted_len = struct.unpack(">Q", raw)[0]

        self._buffer = []  # pending appends to flush_buffer
        self._buf_limit = buffer_size
        self._cache = {}  # in-memory read cache
        self._index_buffer = {}
        self.append = self._buffer.append
        self.buffer_batches = {}

        self.old_header = bytearray(self.HEADER_BYTE_COUNT)  # zero-filled
        self.old_body = bytearray()

    def _puts_gen_batched(self):
        pack_into = self.struct_pack_into
        int_pack = StoredList.int_pack
        batch_size = self.batch_size
        header_bytes = self.HEADER_BYTE_COUNT
        byte_len = self.byte_length

        idx = self._persisted_len
        page = idx // batch_size
        boundary = (page + 1) * batch_size

        running = 0
        if self.old_header:
            header = self.old_header
            body = self.old_body
        else:
            first_page = self._persisted_len // batch_size
            offset_in_page = self._persisted_len % batch_size
            if offset_in_page != 0:
                raw = txn.get(int_pack(first_page))
                header = bytearray(raw[:header_bytes])
                body = bytearray(raw[header_bytes:])
                # compute running from header[offset_in_page]
                running = unpack_from(header, byte_len * offset_in_page)
            else:
                header = self.old_header
                body = self.old_body



        for v in self._buffer:
            if self.cache_on_set:
                self._cache[idx] = v
            # did we cross into the next batch?
            if idx >= boundary:
                # write the final end-offset for the previous page
                slot_idx = (idx - 1) % batch_size
                pack_into(header, byte_len * (slot_idx + 1), running)
                # emit it
                yield (int_pack(page), memoryview(header + body))

                # start next page
                page = idx // batch_size
                boundary = (page + 1) * batch_size
                header = bytearray(header_bytes)
                body = bytearray()
                running = 0

            # record start-offset of this slot
            slot = idx % batch_size
            pack_into(header, byte_len * slot, running)

            # append the data
            b = v
            body.extend(b)
            running += len(b)

            idx += 1

        # flush the last (partial) page
        if running or body:
            slot_idx = (idx - 1) % batch_size
            pack_into(header, byte_len * (slot_idx + 1), running)
            yield (int_pack(page), memoryview(header + body))

        self.old_header, self.old_body = header, body

    def _puts_gen_single(self):
        idx = self._persisted_len
        for v in self._buffer:
            yield (StoredList.int_pack(idx), v)
            idx += 1


    def _sets_gen_batched(self):
        idx = self._persisted_len
        batches = {}
        batch_size = self.batch_size
        for i in self._index_buffer:
            cell = i // batch_size
            if not cell in batches:
                batches[cell] = []


    def _flush_buffer(self) -> None:
        # write buffered items to lmdb
        if not self._buffer:
            return

        total = len(self._buffer) + self._persisted_len
        with self.env.begin(write=True, db=self._db, buffers=True) as txn:
            cursor = txn.cursor()
            cursor.putmulti(append=True, items=self._puts_gen_batched() if self.do_batch_writes else self._puts_gen_single())
        with self.env.begin(write=True, db=self.stat) as txn:
            txn.put(
                    StoredList.LENGTH_KEY,
                    struct.pack(">Q", total)
                )
        self._persisted_len = total
        self._buffer.clear()

    def __len__(self) -> int:
        # total items including buffered
        return self._persisted_len + len(self._buffer)

    def __getitem__(self, index: int) -> str:
        # support negative and cached reads
        length = len(self)
        if index < 0:
            index += length
        if not 0 <= index < length:
            raise IndexError("index out of range")
        if index in self._cache:
            return self._cache[index]
        if index >= self._persisted_len:
            return self._buffer[index - self._persisted_len]

        if self.do_batch_writes:
            # figure page and slot
            page, slot = divmod(index, self.batch_size)
            key = StoredList.int_pack(page)
        else:
            # no expensive divmod
            key = StoredList.int_pack(index)

        # fetch the packed page
        with env.begin(db=self._db, write=False, buffers=True) as txn:
            blob = txn.get(key)
        if blob is None:
            raise IndexError(f"{index} out of range")

        if not self.do_batch_writes:
            return blob

        if blob is None:
            raise IndexError(f"{index} out of range")

        mv = memoryview(blob)
        # read start/end offsets directly from header
        start = self.struct_unpack_from(mv, self.byte_length * slot)[0]
        end = self.struct_unpack_from(mv, self.byte_length * (slot + 1))[0]

        # slice out the data
        data = mv[self.HEADER_BYTE_COUNT + start: self.HEADER_BYTE_COUNT + end]

        if data is None:
            raise ValueError("db corrupted")
        self._cache[index] = data
        return data

    def __setitem__(self, index: int, value: bytes) -> None:
        # update cache, and buffer or lmdb depending on position
        length = len(self)
        if index < 0:
            index += length
        if not 0 <= index < length:
            raise IndexError("assignment index out of range")

        if index >= self._persisted_len:
            # if in active buffer space, write into it
            self._buffer[index - self._persisted_len] = value
        else:
            # otherwise, write into dict
            self._index_buffer[index] = value
        if self.cache_on_set:
            self._cache[index] = value

    def __iter__(self):
        cursor = None
        for i in range(len(self)):
            v = None
            if cursor:
                v = next(cursor)
            if i in self._cache:
                # if index in cached, read corresp.val from your cache
                yield self._cache[i]
            else:
                # otherwise, create cursor
                if not cursor:
                    cursor = env.begin(db=self._db, write=False).cursor(db=self._db).iternext(keys=False, values=True)
                    for j in range(i + 1):
                        v = next(cursor)
                self._cache[i] = v
                yield v
        for v in self._buffer:
            yield v

    def __repr__(self) -> str:
        return f"<StoredList len={len(self)}>"


from time import perf_counter
class StoredDict(StoredObject):
    # LMDB-backed dict with buffered writes and read cache.

    def __init__(self, buffer_size: int = 0, **kwargs):
        # initialize lmdb db, buffers and cache
        super().__init__(**kwargs)
        name = kwargs["name"]
        self._db = self.env.open_db(name.encode("ascii"), create=True)
        self._buf_limit = buffer_size
        self._put_buffer = {}  # pending sets
        self._del_buffer = set()  # pending deletes
        self._cache = {}  # in-memory read cache
        self.absent = set([])

    def __setitem__(self, key: bytes, value) -> None:
        # buffer set and update cache
        if self.cache_on_set:
            self._cache[key] = value
        self._put_buffer[key] = value
        if self._del_buffer:
            self._del_buffer.discard(key)
        if self._buf_limit and len(self._put_buffer) >= self._buf_limit:
            self.flush_buffer()

    def __getitem__(self, key: bytes):
        # retrieve from cache, buffer or lmdb
        if key in self._cache:
            return self._cache[key]
        if key in self._put_buffer:
            return self._put_buffer[key]
        if key in self._del_buffer:
            raise KeyError(key)
        data = env.begin(db=self._db, write=False).get(key)
        if data is None:
            raise KeyError(key)
        self._cache[key] = data
        return data

    def __delitem__(self, key: bytes) -> None:
        # buffer delete and update cache
        if key in self._put_buffer:
            self._put_buffer.pop(key)
        self._del_buffer.add(key)
        if self._cache:
            self._cache.pop(key, None)
        if self._buf_limit and len(self._del_buffer) >= self._buf_limit:
            self.flush_buffer()

    def _puts_gen(self):
        for i in self._put_buffer.keys():
            yield (i, self._put_buffer[i])

    def _flush_buffer(self) -> None:
        # apply buffered sets and deletes to lmdb
        if not self._put_buffer and not self._del_buffer:
            return
        with self.env.begin(write=True, db=self._db, buffers=True) as txn:
            cursor = txn.cursor()
            cursor.putmulti(append=True, items=self._puts_gen())
        for key in self._del_buffer:
            txn.delete(key)
            self.absent.add(key)
        self._put_buffer.clear()
        self._del_buffer.clear()

    def __len__(self) -> int:
        with self.env.begin(db=self._db) as txn:
            return int(txn.stat(db=self._db)["entries"])

    def iterate(self, keys=True, values=False):
        # iterate keys, with cache reading
        with self.env.begin(db=self._db, write=False) as txn:
            cursor = txn.cursor(db=self._db).iternext(keys=keys, values=values)
            for element in cursor:
                key = None
                if keys:
                    key = element if keys and not values else element[0]
                value = None
                if values:
                    value = element if values and not keys else element[1]

                if key in self._del_buffer: continue
                if key in self._cache:
                    yield (key if keys else None, self._cache[key] if values else None)
                    continue
                if values:
                    if key is not None:
                        self._cache[key] = value
                yield (key if keys else None, value)

        if keys and values:
            yield from self._put_buffer.items()
        elif keys:
            yield from self._put_buffer.keys()
        else:
            yield from self._put_buffer.values()

    def __iter__(self):
        raise NameError('Use method "iterate" instead')

    def __contains__(self, key: bytes) -> bool:
        # check presence considering buffers
        if key in self._cache or key in self._put_buffer:
            return True
        if key in self.absent:
            return False
        got = env.begin(db=self._db, write=False).get(key)
        if got is None:
            self.absent.add(key); return False
        self._cache[key] = got
        return True

    def __repr__(self) -> str:
        return f"<StoredDict entries={len(self)}>"


class ScalableStoredDict:
    # Shards a large StoredDict into multiple smaller ones.
    # delegates all dict ops to the correct shard based on
    # your strategy.
    # Makes operations 2x slower, but the overall perfomance
    # won't deteriorate with length growth.
    # Use in cases where you have hundreds of millions of
    # entries.

    def __init__(
        self,
        name: str,
        sharding_call: callable,
        buffer_size: int = 0,
        num_shards: int = 1,
        env_size_mb: int = 0, # Will create separate environments for each shard if not 0
        **stored_kwargs
    ):
        if not name:
            raise ValueError("name must be non-empty")
        if name.lower() == "db":
            raise NameError("name can't be 'db'")

        self.shards: list[StoredDict] = []
        self.num_shards = num_shards
        if env_size_mb and not os.path.isdir("db/"+name):
            os.mkdir("db/"+name)
        for i in range(num_shards):
            shard_name = f"{name}_shard_{i}"
            new_env = env if not env_size_mb else open_environment(f"{name}/{i}_env", size_mb=env_size_mb, lock_safe=False, max_variables=1)
            self.shards.append(StoredDict(name=shard_name, buffer_size=buffer_size, env=new_env, **stored_kwargs))
        # default to simple hash‐mod
        self.sharding_call = sharding_call
        self.length = sum(len(s) for s in self.shards)


    def __setitem__(self, key: bytes, value):
        shard = self.shards[self.sharding_call(key)]
        prev = shard.length
        shard[key] = value
        self.length += shard.length - prev

    def __getitem__(self, key: bytes):
        shard = self.shards[self.sharding_call(key)]
        return shard[key]

    def __delitem__(self, key: bytes):
        shard = self.shards[self.sharding_call(key)]
        prev = shard.length
        del shard[key]
        self.length += shard.length - prev

    def __contains__(self, key: bytes) -> bool:
        return key in self.shards[self.sharding_call(key)]

    def __len__(self) -> int:
        # sum of all shard sizes
        return self.length

    def flush_buffer(self, threaded=False):
        for s in self.shards:
            s.flush_buffer(threaded)

    def keys(self):
        for s in self.shards:
            for pack in s.iterate(keys=True, values=False):
                yield pack[0]

    def values(self):
        for s in self.shards:
            for pack in s.iterate(keys=False, values=True):
                yield pack[1]

    def items(self):
        for s in self.shards:
            yield from s.iterate(keys=True, values=True)

    def __iter__(self):
        yield from self.keys()

    def __repr__(self):
        return f"<ScalableStoredDict shards={self.num_shards} total_entries={len(self)}>"

class ScalableStoredList:
    # Shards a large StoredList into multiple smaller ones.
    # delegates all ops to the correct shard based on
    # Round-Robin strategy.
    # Makes operations 2x slower, but the overall perfomance
    # won't deteriorate with length growth.
    # Use in cases where you have tens of millions of
    # entries.

    def __init__(
        self,
        name: str,
        buffer_size: int = 0,
        num_shards: int = 1,
        **stored_kwargs
    ):
        if not name:
            raise ValueError("name must be non-empty")
        self.num_shards = num_shards
        # create one StoredList per shard
        self.shards = [
            StoredList(buffer_size=buffer_size, name=f"{name}_shard_{i}", **stored_kwargs)
            for i in range(num_shards)
        ]
        self.length = sum(len(s) for s in self.shards)

    def append(self, value: bytes) -> None:
        # global index before append
        shard_idx = self.length % self.num_shards
        self.shards[shard_idx].append(value)
        self.length += 1

    def __len__(self) -> int:
        return self.length


    def __getitem__(self, index: int) -> str:
        if index < 0:
            index += self.length
        if not 0 <= index < self.length:
            raise IndexError("index out of range")
        shard_idx = self.length % self.num_shards
        local_idx = index // self.num_shards
        return self.shards[shard_idx][local_idx]

    def __setitem__(self, index: int, value: str) -> None:
        if index < 0:
            index += self.length
        if not 0 <= index < self.length:
            raise IndexError("assignment index out of range")
        shard_idx = self.length % self.num_shards
        local_idx = index // self.num_shards
        self.shards[shard_idx][local_idx] = value

    def flush_buffer(self, threaded=False) -> None:
        for shard in self.shards:
            shard.flush_buffer(threaded)

    def __iter__(self):
        for shard in self.shards:
            yield from shard

    def __repr__(self) -> str:
        return f"<ScalableStoredList shards={self.num_shards} total_entries={len(self)}>"