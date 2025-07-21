# Nyada 1.0.0
# Speed benchmark:

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
    readahead=False,
    metasync=lock_safe,
    sync=lock_safe
)

env = open_environment("db", 4096, False) # 1 GiB

class StoredReference:
    references = {}
    @staticmethod
    def register(instance: "StoredObject"):
        StoredReference.references[instance.name] = instance
    def __init__(self, name: bytes):
        self.name = name
    @staticmethod
    def from_reference(name: bytes) -> "StoredObject":
        return references[name]
class Types:
    TYPE_STR = str
    TYPE_INT = int
    TYPE_BYTES = bytes
    TYPE_NONE = type(None)
    TYPE_SREF = StoredReference


def encode_val(value) -> bytes:
    match type(value):
        case Types.TYPE_STR:
            return b's' + value.encode('ascii')
        case Types.TYPE_INT:
            return b'i' + str(value).encode('ascii')
        case Types.TYPE_BYTES:
            return b'b' + value
        case Types.TYPE_SREF:
            return b'r' + value.name
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
        case 114: # ord('r')
            return StoredReference.from_name(payload)
        case _:
            raise ValueError(f"unknown typecode: {chr(code)}")


class StoredObject:
    __slots__ = ("variant_typed", "flushing_thread", "name", "cache_on_set", "env", "stat", "stat_cache")
    stat_fields = {b"length": b"0", b"type": b"0", b"batch_writes": b"0"}

    def _map_stat(self, txn):
        pass

    def map_stat(self):
        fields = StoredObject.stat_fields.copy() | self.__class__.stat_fields
       # print("stat..")
      #  print(self.env.stat())

        with self.env.begin(db=self.stat, write=True) as txn:
            cursor = txn.cursor()
            gets = dict(cursor.getmulti(keys=fields.keys()))
            puts = {}
            for field in fields:
                if not field in gets:
                    puts[field] = fields[field]
            self.stat_cache |= gets | puts
            cursor.putmulti(items=puts.items())
            self._map_stat(txn)
       # print(self.env.stat())

        #print(self.stat_cache)


    def write_stat(self, key: bytes, value: bytes):
        if key in self.stat_cache and self.stat_cache[key] == value: return
        self.stat_cache[key] = value
        with self.env.begin(db=self.stat, write=True) as txn:
            txn.put(key, value)

    def get_stat(self, key: bytes) -> bytes:
        if key in self.stat_cache: return self.stat_cache[key]
        with self.env.begin(db=self.stat) as txn:
            res = txn.get(key)
            self.stat_cache[key] = res
            return res
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


    def __init__(self,
                 name="",                 # - db name
                 env=env,                 # - dbs personal env (if not set, will use global env)
                 cache_on_set=False,      # - auto cache on __setitem__ calls at cost of perfomance
                 max_length=0,            # - maximal element length (for batch writes)
                 batch_writes=0,          # - will put multiple values under same key for fewer PUT requests
                                          # This can give perfomance boost for appends and reads/writes
                                          # in nearby indexes. The perfomance will decrease with element size
                                          # growth, so it's a good option for smaller items.
                 constant_length=False,   # - if True, and batch_writes=True, will significantly speed
                                          # up set/get/flush ops, with an assumption that length of
                                          # every byte string is same.
                 ):
        assert name, "Name must be a non-empty string"
        self.name = name
        self.stat = env.open_db((name.decode("ascii") + "__stat").encode("ascii"), create=True)

        self.env = env
        self.cache_on_set = cache_on_set
        self.do_batch_writes = bool(batch_writes)
        self.batch_size = batch_writes
        self.flushing_thread = None
        self.constant_length = constant_length
        self.max_length = max_length

        self.byte_length = 8
        self.HEADER_SLOT_COUNT = batch_writes + 1 # N items + final end-offset
        self.HEADER_BYTE_COUNT = self.HEADER_SLOT_COUNT * self.byte_length  # 1 short = 2 bytes

        self.struct_pack_into = struct.Struct(">Q").pack_into
        self.struct_unpack_from = struct.Struct(">Q").unpack_from

        self.stat_cache = {}
        self.typecode = "Q"
        self.struct_unpack = ">QQ"

        self.pack_format = f"={self.batch_size+1}Q"

        self.map_stat()


from array import array

class StoredList(StoredObject):
    # LMDB-backed list with in-memory buffer and cache.
    # Append-only.

    int_pack = lambda index: struct.pack("=Q", index)
    stat_fields = {b"type": b"1"}

    def _map_stat(self, txn):
        if self.do_batch_writes:
            raw = self.get_stat(b"length")
            self._persisted_len = int(raw.decode())

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        name = kwargs["name"]
        self._batched_writes = {}
        # initialize lmdb db, buffer and cache
        self._db = env.open_db(name, create=True, integerkey=True)
        if not self.do_batch_writes:
            with self.env.begin(db=self._db) as txn:
                self._persisted_len = int(txn.stat(db=self._db)["entries"])

        self._buffer = []  # pending appends to flush_buffer
        self._cache = {}  # in-memory read cache
        self._index_buffer = {}
        self.append = self._buffer.append
        self.buffer_batches = {}

        self.old_header = bytearray(self.HEADER_BYTE_COUNT)  # zero-filled
        self.old_body = bytearray()
        #self._zero_header = bytearray(self.HEADER_BYTE_COUNT)
        self.fetched_blobs = {}


    def _puts_gen_batched(self):
        int_pack = StoredList.int_pack
        batch_size = self.batch_size
        byte_len = self.byte_length

        # where we left off
        page = self._persisted_len // batch_size
        offset_in_page = self._persisted_len % batch_size

        # prepare header array (batch_size+1 slots: start offsets + final end offset)
        header_arr = array('Q', [0] * (batch_size + 1))
        body_chunks = []

        # if there's an in-mem partial page, reload it
        if self.old_header:
            # old_header is a bytearray of length (batch_size+1)*byte_len
            header_arr = array('Q')
            header_arr.frombytes(self.old_header)
            # old_body is a bytearray of existing data
            body_chunks = [bytes(self.old_body)]
            running = header_arr[offset_in_page]
        else:
            running = 0
            # if we’re starting mid-page, fetch existing page
            if offset_in_page:
                with self.env.begin(db=self._db, write=False, buffers=True) as txn:
                    raw = txn.get(int_pack(page)) or b''
                # split header / body out of raw
                hdr_bytes = raw[: byte_len * (batch_size + 1)]
                body_bytes = raw[byte_len * (batch_size + 1):
                                 byte_len * (batch_size + 1) + offset_in_page * byte_len]
                header_arr = array('Q')
                header_arr.frombytes(hdr_bytes)
                body_chunks = [body_bytes]
                running = header_arr[offset_in_page]

        slot = offset_in_page
        idx = self._persisted_len

        for v in self._buffer:
            # if self.cache_on_set:
            #     self._cache[idx] = v

            # mark start-offset for this slot
            header_arr[slot] = running
            body_chunks.append(v)
            running += len(v)

            slot += 1
            idx += 1

            # once we've filled batch_size items, flush
            if slot == batch_size:
                header_arr[batch_size] = running
                data = header_arr.tobytes() + b''.join(body_chunks)
                yield (int_pack(page), memoryview(data))

                # reset for next page
                page += 1
                slot = 0
                running = 0
                body_chunks.clear()

        # flush any remaining partial page
        if slot != offset_in_page:
            header_arr[slot] = running
            data = header_arr.tobytes() + b''.join(body_chunks)
            yield (int_pack(page), memoryview(data))

        # save header/body for next invocation
        self.old_header = bytearray(header_arr.tobytes())
        self.old_body = bytearray().join(body_chunks)

    def _puts_gen_single(self):
        # usual yield
        idx = self._persisted_len
        for v in self._buffer:
            yield (StoredList.int_pack(idx), v)
            idx += 1

    def _puts_gen_constant(self):
        int_pack = StoredList.int_pack
        bs, bl = self.batch_size, self.max_length

        # figure out where we left off
        start_page = self._persisted_len // bs
        start_off = self._persisted_len % bs

        # load any existing partial‐page bytes
        if self.old_body:
            old_bytes = bytes(self.old_body)
        elif start_off:
            with self.env.begin(db=self._db, write=False) as txn:
                raw = txn.get(int_pack(start_page)) or b''
            old_bytes = raw[: start_off * bl]
        else:
            old_bytes = b''

        buf = self._buffer
        total_items = start_off + len(buf)
        total_pages = (total_items + bs - 1) // bs
        if total_items == start_off and not buf:
            return
        # stream out each page
        for p in range(total_pages):
            page = start_page + p
            gstart = p * bs
            gend = min(gstart + bs, total_items)

            # slice of new‐buffer for this page
            bstart = max(0, gstart - start_off)
            bend = gend - start_off

            if p == 0 and start_off:
                # first page: prefix whatever was left over
                chunk = old_bytes + b''.join(buf[:bend])
            else:
                # subsequent pages: only join the new records
                chunk = b''.join(buf[bstart:bend])
            # print(page)
            yield int_pack(page), memoryview(chunk)

    def _sets_gen(self):
        for idx, value in self._index_buffer.items():
            yield StoredList.int_pack(idx), value

    def _sets_gen_batched(self):
        int_pack = StoredList.int_pack
        bs = self.batch_size
        hdr_bytes = self.HEADER_BYTE_COUNT
        # read-only txn for fetching current pages
        txn = self.env.begin(db=self._db, write=False, buffers=True)

        for page,blob in self.get_results:
            assigns = self._batched_writes[page]

            if self.constant_length:
                # fixed-size slots, just overwrite the slice
                bl = self.max_length
                chunk = bytearray(blob)
                for slot, v in assigns.items():
                    if len(v) != bl:
                        raise ValueError(f"slot {slot}: expected length {bl}, got {len(v)}")
                    start = slot * bl
                    chunk[start:start+bl] = v
            else:
                # variable-length: rebuild header + body
                header = blob[:hdr_bytes]
                offsets = struct.unpack(self.pack_format, header)  # tuple of bs+1 offsets

                # collect each slot’s bytes (new or old)
                body_chunks = []
                for slot in range(bs):
                    if slot in assigns:
                        chunk_bytes = assigns[slot]
                    else:
                        start = hdr_bytes + offsets[slot]
                        end = hdr_bytes + offsets[slot + 1]
                        chunk_bytes = blob[start:end]
                    body_chunks.append(chunk_bytes)

                # rebuild header array
                new_header = array('Q', [0] * (bs + 1))
                running = 0
                for i, chunk_bytes in enumerate(body_chunks):
                    new_header[i] = running
                    running += len(chunk_bytes)
                new_header[bs] = running

                # concat and yield
                chunk = new_header.tobytes() + b''.join(body_chunks)

            yield page, memoryview(chunk)


    def _flush_buffer(self) -> None:
        # write buffered items to lmdb
        if not self._buffer and not self._batched_writes:
            return

        total = len(self._buffer) + self._persisted_len
        with self.env.begin(write=True, db=self._db, buffers=True) as txn:
            cursor = txn.cursor()
            call = self._puts_gen_single
            if self.do_batch_writes:
                if self.constant_length:
                    call = self._puts_gen_constant
                else:
                    call = self._puts_gen_batched
            cursor.putmulti(append=True, items=call())

            call_sets = self._sets_gen
            if self.do_batch_writes:
                call_sets = self._sets_gen_batched

            self.get_results = cursor.getmulti(self._batched_writes.keys())

            cursor.putmulti(items=call_sets())

        if self.do_batch_writes:
            self.write_stat(key=b"length", value=str(total).encode("ascii"))
        self._persisted_len = total
        self._batched_writes.clear()
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
            #print(page)
            key = StoredList.int_pack(page)
        else:
            key = StoredList.int_pack(index)

        # fetch the packed page

        if not self.do_batch_writes:
            with self.env.begin(db=self._db, write=False, buffers=True) as txn:
                blob = txn.get(key)
        elif not key in self.fetched_blobs:
            with self.env.begin(db=self._db, write=False, buffers=True) as txn:
                blob = txn.get(key)
            self.fetched_blobs[key] = blob
        else:
            blob = self.fetched_blobs[key]

        if blob is None: raise IndexError(f"{index} out of range")

        if not self.do_batch_writes:
            value = blob
        elif self.constant_length:
            value = blob[slot * self.max_length : (slot+1) * self.max_length]
        else:
            # array of unsigned offsets
            hdr_view = blob[:self.HEADER_BYTE_COUNT]
            offsets = struct.unpack(self.pack_format, hdr_view)
            start, end = offsets[slot], offsets[slot+1]

            body_offset = self.HEADER_BYTE_COUNT
            value = blob[body_offset + start : body_offset + end]

        if self.cache_on_set:
            self._cache[index] = value
        return value

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
            if self.do_batch_writes:
                res = divmod(index, self.batch_size)
                page, slot = StoredList.int_pack(res[0]), res[1]
                if not page in self._batched_writes:
                    self._batched_writes[page] = {slot: value}
                else:
                    self._batched_writes[page][slot] = value
            else:
                self._index_buffer[index] = value
        if self.cache_on_set:
            self._cache[index] = value

    def _default_iter(self):
        cursor = env.begin(db=self._db, write=False).cursor(db=self._db).iternext(keys=False, values=True)
        cursor_index = 0
        for i in range(len(self)):
            if i in self._cache:
                # if index in cached, read corresp.val from your cache
                yield self._cache[i]
            else:
                # otherwise, iter on cursor
                n = i - cursor_index
                if n != 1:
                    for j in range(n):
                        cursor_index += 1
                        v = next(cursor)
                else:
                    v = next(cursor)

                self._cache[i] = v
                yield v

    def _batch_iter(self):
        cursor = env.begin(db=self._db, write=False).cursor(db=self._db).iternext(keys=False, values=True)
        blob = None
        print("DFJJ")
        for i in range(len(self)):
            if i in self._cache: pass#yield self._cache[i]
            if i % self.batch_size == 0:
                blob = next(cursor)
            print(blob)

    def _batch_iter_constant(self):
        pass

    def __iter__(self):
        if self.do_batch_writes:
            if self.constant_length:
                yield from self._batch_iter_constant()
            else:
                yield from self._batch_iter()
        else:
            yield from self._default_iter()
        yield from self._buffer

    def __repr__(self) -> str:
        return f"<StoredList len={len(self)}>"


from time import perf_counter
class StoredDict(StoredObject):
    # LMDB-backed dict with buffered writes and read cache.
    stat_fields = {b"type": b"2"}

    def __init__(self, **kwargs):
        # initialize lmdb db, buffers and cache
        super().__init__(**kwargs)
        name = kwargs["name"]
        self._db = self.env.open_db(name, create=True)
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

    def _puts_gen(self):
        for i in self._put_buffer.keys():
            yield (i, (self._put_buffer[i]))

    def _flush_buffer(self) -> None:
        # apply buffered sets and deletes to lmdb
        if not self._put_buffer and not self._del_buffer:
            return
        with self.env.begin(write=True, db=self._db, buffers=True) as txn:
            cursor = txn.cursor()
            cursor.putmulti(items=self._put_buffer.items())
        for key in self._del_buffer:
            cursor.delete(key)
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
            self.shards.append(StoredDict(name=shard_name, env=new_env, **stored_kwargs))
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
        num_shards: int = 1,
        **stored_kwargs
    ):
        if not name:
            raise ValueError("name must be non-empty")
        self.num_shards = num_shards
        # create one StoredList per shard
        self.shards = [
            StoredList(name=f"{name}_shard_{i}", **stored_kwargs)
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