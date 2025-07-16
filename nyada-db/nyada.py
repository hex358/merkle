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

    def __init__(self, variant_typed=False, name="", env=env, cache_on_set=True):
        assert name, "Name must be a non-empty string"
        self.env = env
        self.cache_on_set = cache_on_set
        self.variant_typed = variant_typed
        self.flushing_thread = None

    @staticmethod
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

    @staticmethod
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


class StoredList(StoredObject):
    # LMDB-backed list with in-memory buffer and cache.
    # Only strings are supported, as only they were
    # used as list data type in this project.
    # Append-only.

    int_pack = lambda index: struct.pack(">Q", index)

    def __init__(self, buffer_size: int = 0, **kwargs):
        super().__init__(**kwargs)
        name = kwargs["name"]
        # initialize lmdb db, buffer and cache
        self._db = env.open_db(name.encode("ascii"), create=True)
        with self.env.begin(db=self._db) as txn:
            self._persisted_len = txn.stat(db=self._db)["entries"]
        self._buffer = []  # pending items to flush_buffer
        self._buf_limit = buffer_size
        self._cache = {}  # in-memory read cache

    def append(self, value: str) -> None:
        # add item to buffer and flush_buffer if limit reached
        self._buffer.append(value)
        if self.cache_on_set:
            self._cache[idx] = v
        if self._buf_limit and len(self._buffer) >= self._buf_limit:
            self.flush_buffer()

    def _flush_buffer(self) -> None:
        # write buffered items to lmdb
        if not self._buffer:
            return

        with self.env.begin(write=True, db=self._db) as txn:
            idx = self._persisted_len
            stack = b""
            for v in self._buffer:
                key = StoredList.int_pack(idx)
                stack += StoredObject.encode_val(v) if self.variant_typed else v
                txn.put(key, stack,
                        append=True)  # , flags=txn.)
                idx += 1
        self._persisted_len = idx
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
        with self.env.begin(db=self._db, write=False) as txn:
            data = txn.get(StoredList.int_pack(index))
        if data is None:
            raise ValueError("db corrupted")
        result = StoredObject.decode_val(data) if self.variant_typed else data
        self._cache[index] = result
        return result

    def __setitem__(self, index: int, value: str) -> None:
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
            if index in self._cache and self._cache[index] == value:
                return
            # otherwise, put into db
            with self.env.begin(write=True, db=self._db) as txn:
                txn.put(StoredList.int_pack(idx),
                        StoredObject.encode_val(value) if self.variant_typed else value)
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
                decoded = v if not self.variant_typed else StoredObject.decode_val(v)
                self._cache[i] = decoded
                yield decoded
        for v in self._buffer:
            yield v

    def __repr__(self) -> str:
        return f"<StoredList len={len(self)}>"


from time import perf_counter
class StoredDict(StoredObject):
    # LMDB-backed dict with buffered writes and read cache.
    # Keys are strings, values can be one of the following
    # types:
    # None, str, int, bytes

    def __init__(self, buffer_size: int = 0, **kwargs):
        # initialize lmdb db, buffers and cache
        super().__init__(**kwargs)
        name = kwargs["name"]
        self._db = self.env.open_db(name.encode("ascii"), create=True)
        self._buf_limit = buffer_size
        self._put_buffer = {}  # pending sets
        self._del_buffer = set()  # pending deletes
        self._cache = {}  # in-memory read cache
        with self.env.begin(db=self._db) as txn:
            self.length = int(txn.stat(db=self._db)["entries"])

    def __setitem__(self, key: str, value) -> None:
        # buffer set and update cache
        if self.cache_on_set:
            self.length += 1
            self._cache[key] = value
        self._put_buffer[key] = value
        if self._del_buffer:
            self._del_buffer.discard(key)
        if self._buf_limit and len(self._put_buffer) >= self._buf_limit:
            self.flush_buffer()

    def __getitem__(self, key: str):
        # retrieve from cache, buffer or lmdb
        if key in self._cache:
            return self._cache[key]
        if key in self._put_buffer:
            return self._put_buffer[key]
        if key in self._del_buffer:
            raise KeyError(key)
        data = env.begin(db=self._db, write=False).get(key.encode("ascii"))
        if data is None:
            raise KeyError(key)
        value = StoredObject.decode_val(data) if self.variant_typed else data.decode("ascii")
        self._cache[key] = value
        return value

    def __delitem__(self, key: str) -> None:
        # buffer delete and update cache
        if key in self._put_buffer:
            self._put_buffer.pop(key)
        if not key in self._del_buffer:
            self.length -= 1
            self._del_buffer.add(key)
        if self._cache:
            self._cache.pop(key, None)
        if self._buf_limit and len(self._del_buffer) >= self._buf_limit:
            self.flush_buffer()

    def _flush_buffer(self) -> None:
        # apply buffered sets and deletes to lmdb
        if not self._put_buffer and not self._del_buffer:
            return
        with self.env.begin(write=True, db=self._db, ) as txn:
            for key, val in self._put_buffer.items():
                txn.put(key.encode("ascii"),
                        StoredObject.encode_val(val) if self.variant_typed else val.encode("ascii"))
            for key in self._del_buffer:
                txn.delete(key.encode("ascii"))
        self._put_buffer.clear()
        self._del_buffer.clear()

    def __len__(self) -> int:
        return self.length

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

                key = key.decode()
                if key in self._del_buffer: continue
                if key in self._cache:
                    yield (key if keys else None, self._cache[key] if values else None)
                    continue
                decoded_val = None
                if values:
                    decoded_val = StoredObject.decode_val(value) if self.variant_typed else value.decode("ascii")
                    if key is not None:
                        self._cache[key] = decoded_val
                yield (key if keys else None, decoded_val)

        if keys and values:
            yield from self._put_buffer.items()
        elif keys:
            yield from self._put_buffer.keys()
        else:
            yield from self._put_buffer.values()

    def __iter__(self):
        raise NameError('Use method "iterate" instead')

    def __contains__(self, key: str) -> bool:
        # check presence considering buffers
        if key in self._cache:
            return True
        return env.begin(db=self._db, write=False).get(key.encode("ascii")) is not None

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


    def __setitem__(self, key: str, value):
        shard = self.shards[self.sharding_call(key)]
        prev = shard.length
        shard[key] = value
        self.length += shard.length - prev

    def __getitem__(self, key: str):
        shard = self.shards[self.sharding_call(key)]
        return shard[key]

    def __delitem__(self, key: str):
        shard = self.shards[self.sharding_call(key)]
        prev = shard.length
        del shard[key]
        self.length += shard.length - prev

    def __contains__(self, key: str) -> bool:
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

    def append(self, value: str) -> None:
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