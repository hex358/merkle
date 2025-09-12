# HexDB 1.0.0

import os
import struct
import threading
import lmdb
from threading import Thread
import db_boosts
from dataclasses import dataclass


if not os.path.isdir(".db"):
    # create database directory if it does not exist
    os.mkdir(".db")


def open_environment(name: str,
                     size_mb: float,
                     lock_safe: bool = True,
                     max_variables: int = 1024):
    """
    Opens LMDB environment with given parameters
    """
    return lmdb.open(
        ".db/" + name,
        max_dbs=max_variables,
        map_size=size_mb * 1024 ** 2,
        writemap=True,
        map_async=True,
        readahead=False,
        metasync=lock_safe,
        sync=lock_safe
    )

global_env = None

def Start(*args, **kwargs):
    global global_env
    global_env = open_environment(*args, **kwargs)


@dataclass(frozen=True)
class BatchingConfig:
    batch_size: int = 0
    constant_length: bool = False
    max_item_length: int = 0
    on: bool = True
    def __post_init__(self):
        if self.constant_length and not self.max_item_length:
            raise ValueError("you must rovide max_item_length if length is constant")

default_batching_config = BatchingConfig(batch_size=0, constant_length=False, max_item_length=0)

class StoredReference:
    references = {}
    _lock = threading.RLock()

    @staticmethod
    def register(instance: "StoredObject"):
        StoredReference.references[instance.name] = instance

    def __init__(self, instance: "StoredObject"):
        self.name = instance.name
        self.env = instance.env
        StoredReference.references[instance.name] = instance

    @staticmethod
    def from_reference(name: bytes) -> "StoredObject":
        # fast path
        inst = StoredReference.references.get(name)
        if inst is not None:
            return inst

        # slow path: try to re-open from __stat without creating anything new
        with StoredReference._lock:
            inst = StoredReference.references.get(name)
            if inst is not None:
                return inst

            try:
                stat_db = global_env.open_db(name + b"__stat", create=False)
            except lmdb.Error as e:
                raise KeyError(f"StoredObject {name!r} not found (no __stat db)") from e

            with global_env.begin(db=stat_db, write=False) as txn:
                t  = txn.get(b"type")  # b"1" => StoredList, b"2" => StoredDict
                if t is None:
                    raise KeyError(f"StoredObject {name!r} missing 'type' in __stat")

                bw = int(txn.get(b"batch_writes") or b"0") == 1
                bs = int(txn.get(b"bs") or b"0")
                ml = int(txn.get(b"ml") or b"0")
                cl = int(txn.get(b"cl") or b"0") == 1

            cfg = BatchingConfig(
                batch_size=bs,
                constant_length=cl,
                max_item_length=ml,
                on=bw
            )

            if t == b"1":
                inst = StoredList(name=name, env=global_env, batching_config=cfg)
            elif t == b"2":
                inst = StoredDict(name=name, env=global_env, batching_config=cfg)
            else:
                raise KeyError(f"Unknown stored type {t!r} for {name!r}")

            # StoredObject.__init__ will register it, but be explicit:
            StoredReference.register(inst)
            return inst


class Types:
    TYPE_STR = str
    TYPE_INT = int
    TYPE_BYTES = bytes
    TYPE_NONE = type(None)
    TYPE_SREF = StoredReference


def encode_val(value) -> bytes:
    """
    Encodes Python value into bytes with type prefix
    """
    match type(value):
        case Types.TYPE_STR:
            # string type: prefix 's' and ascii-encode
            return b's' + value.encode('ascii')
        case Types.TYPE_INT:
            # integer type: prefix 'i' and ascii-encode
            return b'i' + str(value).encode('ascii')
        case Types.TYPE_BYTES:
            # bytes type: prefix 'b' and include raw bytes
            return b'b' + value
        case Types.TYPE_SREF:
            # stored reference: prefix 'r' and include reference name
            return b'r' + value.name# + b":" + value.env.path()
        case Types.TYPE_NONE:
            # none type: prefix 'n' only
            return b'n'
        case _:
            # unsupported types raise error
            raise TypeError(f"unsupported type: {type(value)}")


def decode_val(data: bytes):
    """
    Decodes bytes with type prefix back into Python value
    """
    code, payload = data[0], data[1:]
    match code:
        case 115:  # ord('s')
            # decode ascii string
            return payload.decode('ascii')
        case 105:  # ord('i')
            # parse ascii integer
            return int(payload)
        case 98:   # ord('b')
            # return raw bytes
            return payload
        case 110:  # ord('n')
            # represent None
            return None
        case 114:  # ord('r')
            # retrieve stored reference by name
            #payloads = payload.split(b":")
            return StoredReference.from_reference(payload)
        case _:
            # ??
            raise ValueError(f"unknown typecode: {chr(code)}")


class StoredObject:
    __slots__ = (
        "variant_typed", "flushing_thread", "name", "cache_on_set",
        "env", "stat", "stat_cache", "do_batch_writes",
        "batch_size", "max_length"
    )
    stat_fields = {b"length": b"0", b"type": b"0", b"batch_writes": b"0"}

    def __init__(self,
                 name: bytes,                    # Name bytestring.
                 env=None,                        # - Custom environment. Global one as default.
                 cache_on_set: bool = False,     # - Cache on __setitem__ calls. Will slow down
                                                 # the SETs, and speed up the GETs.
                 batching_config: BatchingConfig = default_batching_config
                                                 # Batching config. Specify your batching
                                                 # parameters here.
                 ):
        """
        Initializes StoredObject with configuration parameters
        """
        if env is None: env = global_env
        if not batching_config.on:
            batching_config = default_batching_config
        assert name, "Name must be a non-empty bytestring"
        self.env = env

        self.name = name
        self.stat = self.env.open_db(name + b"__stat", create=True)

        self.cache_on_set = cache_on_set
        self.do_batch_writes = bool(batching_config.batch_size)
        self.batch_size = batching_config.batch_size
        self.flushing_thread = None
        self.constant_length = batching_config.constant_length
        self.max_length = batching_config.max_item_length

        self.byte_length = 8
        self.HEADER_SLOT_COUNT = batching_config.batch_size + 1  # N items + final end-offset
        self.HEADER_BYTE_COUNT = self.HEADER_SLOT_COUNT * self.byte_length  # 1 short = 2 bytes

        self.struct_pack_into = struct.Struct(">Q").pack_into
        self.struct_unpack_from = struct.Struct(">Q").unpack_from

        self.stat_cache = {}
        self.typecode = "Q"
        self.struct_unpack = ">QQ"

        self.pack_format = f"={self.batch_size + 1}Q"

        self.map_stat()

        self.write_stat(b"batch_writes", b"1" if self.do_batch_writes else b"0")
        self.write_stat(b"bs", str(self.batch_size).encode("ascii"))
        self.write_stat(b"ml", str(self.max_length).encode("ascii"))
        self.write_stat(b"cl", b"1" if self.constant_length else b"0")
        StoredReference.register(self)

    def _map_stat(self, txn):
        """
        For subclasses to map additional stats
        """
        pass

    def map_stat(self):
        """
        Maps and initializes stat fields in LMDB
        """
        fields = StoredObject.stat_fields.copy() | self.__class__.stat_fields
        with self.env.begin(db=self.stat, write=True) as txn:
            cursor = txn.cursor()
            existing = dict(cursor.getmulti(keys=fields.keys()))
            missing = {k: v for k, v in fields.items() if k not in existing}
            self.stat_cache |= existing | missing
            cursor.putmulti(items=missing.items())
            self._map_stat(txn)

    def write_stat(self, key: bytes, value: bytes):
        """
        Writes a stat key/value pair if changed
        """
        if key in self.stat_cache and self.stat_cache[key] == value:
            return
        self.stat_cache[key] = value
        with self.env.begin(db=self.stat, write=True) as txn:
            txn.put(key, value)

    def get_stat(self, key: bytes) -> bytes:
        """
        Retrieves a stat value, caching it in memory
        """
        if key in self.stat_cache:
            return self.stat_cache[key]
        with self.env.begin(db=self.stat) as txn:
            result = txn.get(key)
        self.stat_cache[key] = result
        return result

    def _flush_buffer(self):
        """
        for subclasses to flush their buffers
        """
        pass

    def wait_for_flush(self):
        """
        Waits for any background flush thread to complete
        """
        if self.flushing_thread is not None:
            # join existing flush thread
            self.flushing_thread.join()
            self.flushing_thread = None

    def is_flushing_buffer(self) -> bool:
        """
        Returns True if a flush thread is active
        """
        # check thread alive status
        return bool(self.flushing_thread and self.flushing_thread.is_alive())

    def flush_buffer(self, threaded: bool = False):
        """
        Flushes buffer either synchronously or in a background thread
        """
        # ensure previous flush is done
        if self.flushing_thread is not None:
            self.flushing_thread.join()
            self.flushing_thread = None

        if threaded:
            # start flush in new thread
            self.flushing_thread = Thread(target=self._flush_buffer)
            self.flushing_thread.start()
        else:
            # flush immediately
            self._flush_buffer()

    def __repr__(self) -> str:
        """
        Returns representation of StoredObject
        """
        return f"<StoredObject name={self.name!r}>"


from array import array
from time import perf_counter

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
        self.fetched_headers = {}
        name = kwargs["name"]
        self._batched_writes = {}
        # initialize lmdb db, buffer and cache
        self._db = self.env.open_db(name, create=True, integerkey=True)
        if not self.do_batch_writes:
            with self.env.begin(db=self._db) as txn:
                self._persisted_len = int(txn.stat(db=self._db)["entries"])

        self._buffer = []  # pending appends to flush_buffer
        self._cache = {}  # in-memory read cache
        self._index_buffer = {}
        self.append = self._buffer.append if not self.cache_on_set else self.cached_append
        self.buffer_batches = {}

        self.old_header = bytearray(self.HEADER_BYTE_COUNT)  # zero-filled
        self.old_body = bytearray()
        #self._zero_header = bytearray(self.HEADER_BYTE_COUNT)
        self.fetched_blobs = {}
        self.leftover = []

    def _puts_gen_batched(self):
        int_pack = StoredList.int_pack
        batch_size = self.batch_size
        byte_len = self.byte_length

        # figure out which page and slot we're starting in
        page = self._persisted_len // batch_size
        initial_page = page
        self.last_page = initial_page
        offset_in_page = self._persisted_len % batch_size

        # start with either an existing partial in-mem or load it from disk
        if self.old_body:
            header_arr = array('Q')
            header_arr.frombytes(self.old_header)
            body_chunks = [bytes(self.old_body)]
            running = header_arr[offset_in_page]
        else:
            header_arr = array('Q', [0] * (batch_size + 1))
            body_chunks = []
            if offset_in_page:
                # load the on-disk partial page so we can append to it
                with self.env.begin(db=self._db, write=False) as txn:
                    raw = txn.get(int_pack(page)) or b''
                hdr_size = byte_len * (batch_size + 1)
                hdr_bytes = raw[:hdr_size]
                body_bytes = raw[hdr_size: hdr_size + offset_in_page * byte_len]

                header_arr = array('Q')
                header_arr.frombytes(hdr_bytes)
                body_chunks = [body_bytes]
                running = header_arr[offset_in_page]
            else:
                running = 0

        slot = offset_in_page

        # consume the in-memory buffer
        for v in self._buffer:
            header_arr[slot] = running
            body_chunks.append(v)
            running += len(v)
            slot += 1

            # once we fill a full page, emit or stash it
            if slot == batch_size:
                header_arr[batch_size] = running
                data = header_arr.tobytes() + b''.join(body_chunks)

                cur_page = page
                to_yield = (int_pack(cur_page), memoryview(data))
                if cur_page > initial_page:
                    yield memoryview(to_yield)
                else:
                    self.leftover.append(to_yield)

                # reset for next page
                page += 1
                header_arr = array('Q', [0] * (batch_size + 1))
                body_chunks = []
                slot = 0
                running = 0

        # flush any remaining partial page
        if body_chunks:
            header_arr[slot] = running
            data = header_arr.tobytes() + b''.join(body_chunks)

            cur_page = page
            to_yield = (int_pack(cur_page), memoryview(data))
            if cur_page > initial_page:
                yield memoryview(to_yield)
            else:
                self.leftover.append(to_yield)

            # preserve for the next call
            self.old_header = bytearray(header_arr.tobytes())
            self.old_body = bytearray().join(body_chunks)

    def cached_append(self, value: bytes):
        self._cache[len(self)] = value
        self._buffer.append(value)

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

        # load any existing tail‐bytes
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

        # we'll reassign at the end if the last page is partial
        new_old_body = None

        for p in range(total_pages):
            page = start_page + p
            gstart = p * bs
            gend = min(gstart + bs, total_items)

            # local slice of buffer for this page
            bstart = max(0, gstart - start_off)
            bend = gend - start_off

            if p == 0 and start_off:
                chunk = old_bytes + b''.join(buf[:bend])
            else:
                chunk = b''.join(buf[bstart:bend])

            # remember if we ended on a partial page
            if (p == total_pages - 1) and (gend - gstart < bs):
                new_old_body = chunk

            key = int_pack(page)
            mv = memoryview(chunk)

            if (page > start_page) or (p == 0 and start_off == 0):
                yield key, mv
            else:
                self.leftover.append((key, mv))

        # persist the new partial tail if any
        if new_old_body is not None:
            self.old_body = bytearray(new_old_body)
        else:
            self.old_body = None

    def _sets_gen(self):
        for idx, value in self._index_buffer.items():
            yield StoredList.int_pack(idx), value

    def _sets_gen_batched(self):
        int_pack = StoredList.int_pack
        bs = self.batch_size
        hdr_bytes = self.HEADER_BYTE_COUNT

        for page,blob in self.get_results:
            assigns = self._batched_writes[page]

            if self.constant_length:
                # fixed-size slots, just overwrite the slice
                chunk = db_boosts.patch_constant_length((blob), assigns, self.max_length)
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
            args = ()
            cache = []
            call = self._puts_gen_single
            if self.do_batch_writes:
                if self.constant_length:
                    call = self._puts_gen_constant; args = ()
                else:
                    call = self._puts_gen_batched; args = ()
           # print(args)
            cursor.putmulti(items=(call(*args)), append=True, reserve=1)
           # for i in range(len(a)):
            #    a[i][:] = cache[i][:]
            #print(a)
            if self.leftover:
                cursor.putmulti(append=False, items=self.leftover, reserve=1)
                self.leftover.clear()

            call_sets = self._sets_gen
            if self.do_batch_writes:
                call_sets = self._sets_gen_batched

            self.get_results = cursor.getmulti(self._batched_writes.keys())

            cursor.putmulti(items=call_sets(), reserve=0)

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
            if not key in self.fetched_headers:
                hdr_view = blob[:self.HEADER_BYTE_COUNT]
                offsets = struct.unpack(self.pack_format, hdr_view)
                self.fetched_headers[key] = offsets
            else:
                offsets = self.fetched_headers[key]
            start, end = offsets[slot], offsets[slot+1]
            value = blob[self.HEADER_BYTE_COUNT + start : self.HEADER_BYTE_COUNT + end]

        if self.cache_on_set:
            self._cache[index] = value
        #if value is memoryview: value = bytes(value)
        return value

    def __setitem__(self, index: int, value: bytes) -> None:
        # update cache, and buffer or lmdb depending on position
        length = len(self)
        if index < 0:
            index += length

        if not 0 <= index < length:
            raise IndexError(f"assignment {index} index out of range")

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
        """
        Simple, unbatched iteration
        """
        cursor = self.env.begin(db=self._db, write=False).cursor(db=self._db).iternext(keys=False, values=True)
        cursor_index = 0
        for i in range(self._persisted_len):
            if i in self._cache:
                # if index in cached, read corresp.val from your cache
                yield self._cache[i]
            else:
                # otherwise, iter on cursor
                n = i - cursor_index
                v = None
                if n > 1:
                    for j in range(n):
                        cursor_index += 1
                        v = next(cursor)
                else:
                    cursor_index += 1
                    v = next(cursor)

                self._cache[i] = v
                yield v

    def _batch_iter(self):
        """
        Batched iteration over blobs
        """
        iterate = self.env.begin(db=self._db, write=False).cursor(db=self._db).iternext(keys=False, values=True)
        blob = None; last_cursor_pos = 0
        for i in range(self._persisted_len):
            if i in self._cache: yield self._cache[i]
            slot = i % self.batch_size

            # if entered new slot
            if slot == 0:
                # reinitialize window
                left, right = 0, 0
                n = i // self.batch_size; key = StoredList.int_pack(n)

                if key in self.fetched_blobs:
                    # if fetched, don't use cursor
                    blob = self.fetched_blobs[key]
                else:
                    # otherwise, like usual
                    iters = n - last_cursor_pos
                    if iters > 1:
                        for j in range(iters):
                            last_cursor_pos += 1
                            blob = next(iterate)
                    else:
                        last_cursor_pos += 1
                        blob = next(iterate)
                    self.fetched_blobs[key] = blob
                if not self.constant_length:
                    # unpack dynamically
                    hdr_view = blob[:self.HEADER_BYTE_COUNT]
                    offsets = struct.unpack(self.pack_format, hdr_view)

            # simply increment by length if it's constant, otherwise look into header
            new_right = right + self.max_length if self.constant_length else offsets[slot+1]
            left, right = right, new_right
            if self.constant_length:
                # no header bytes
                yield blob[left:right]
            else:
                # there are header bytes at the beginning of the blob
                yield blob[self.HEADER_BYTE_COUNT + left: self.HEADER_BYTE_COUNT + right]

    def __iter__(self):
        """
        Iteration
        """
        if self.do_batch_writes:
            yield from self._batch_iter()
        else:
            yield from self._default_iter()
        yield from self._buffer


    def __repr__(self) -> str:
        return f"<StoredList len={len(self)}>"


class StoredDict(StoredObject):
    """
    LMDB-backed dict with buffered writes and read cache
    """
    stat_fields = {b"type": b"2"}

    def __init__(self, **kwargs):
        """
        Initializes StoredDict with buffering and cache
        """
        super().__init__(**kwargs)
        name = kwargs["name"]
        self._db = self.env.open_db(name, create=True, integerkey=self.do_batch_writes)
        self._put_buffer = {}
        self._del_buffer = set()
        self._cache = {}
        self.absent = set()
        self.fetched_buckets = {}
        self._buffer = []  # pending appends to flush_buffer
        self._put_buckets = {}
        self._delete_buckets = {}

    def __setitem__(self, key: bytes, value: bytes):
        """
        Buffers a set operation for the given key/value
        """
        if self.cache_on_set:
            self._cache[key] = value

        if not self.do_batch_writes:
            # simple buffer for non-batched writes
            self._put_buffer[key] = value
            self._del_buffer.discard(key)
        else:
            # batched writes go into bucket
            bucket = db_boosts.bucket(key, self.batch_size)
            if bucket in self._delete_buckets and key in self._delete_buckets[bucket]:
                self._delete_buckets[bucket].remove(key)
            if bucket not in self._put_buckets: self._put_buckets[bucket] = {key: value}
            else: self._put_buckets[bucket][key] = value
            if bucket in self.fetched_buckets: self.fetched_buckets[bucket][key] = value

    def __getitem__(self, key: bytes | list) -> bytes | list[bytes]:
        """
        Retrieves value for key from cache or LMDB
        """
        if key in self._cache:
            # if cached
            return self._cache[key]

        if not self.do_batch_writes and key in self._put_buffer:
            return self._put_buffer[key]
        if key in self._del_buffer:
            raise KeyError(key)

        if self.do_batch_writes:
            bucket = db_boosts.bucket(key, self.batch_size)
            # if writes are batched
            if bucket in self._put_buckets and key in self._put_buckets[bucket]:
                val = self._put_buckets[bucket][key]
                self._cache[key] = val
                return val
            if bucket not in self.fetched_buckets:
                raw = self.env.begin(db=self._db, write=False).get(bucket)
                if raw is None:
                    raise KeyError(key)
                # from db_boosts c++ lib
                data = db_boosts.deserialize(raw)
                self.fetched_buckets[bucket] = data
            else:
                data = self.fetched_buckets[bucket]

            if key not in data:
                raise KeyError(key)
            val = data[key]
        else:
            # otherwise, go simple
            raw = self.env.begin(db=self._db, write=False).get(key)
            if raw is None:
                raise KeyError(key)
            val = raw

        self._cache[key] = val
        return val

    def __delitem__(self, key: bytes):
        """
        Buffers a delete operation for the given key
        """
        self._put_buffer.pop(key, None)
        if not self.do_batch_writes:
            self._del_buffer.add(key)
        else:
            bucket = db_boosts.bucket(key, self.batch_size)
            if bucket in self.fetched_buckets: self.fetched_buckets[bucket].pop(key, None)
            if bucket in self._put_buckets: self._put_buckets[bucket].pop(key, None)
            if bucket not in self._delete_buckets: self._delete_buckets[bucket] = {key}
            else: self._delete_buckets[bucket].add(key)
        self._cache.pop(key, None)

    def _puts_gen_batched(self):
        """
        Generator for batched dict puts and deletes
        """
        # partial puts
        for bucket, puts in self._put_buckets.items():
            if bucket in self._bucket_gets:
                got = self.fetched_buckets.get(bucket)
                if got is None:
                    base = db_boosts.deserialize(self._bucket_gets[bucket]); self.fetched_buckets[bucket] = base
                else:
                    base = got
                base.update(puts)
            else:
                base = puts
            yield bucket, memoryview(db_boosts.serialize(base))

        # partial deletes
        for bucket, deletes in self._delete_buckets.items():
            base = self.fetched_buckets.get(bucket) or db_boosts.deserialize(self._deletes_gets[bucket])
            for k in deletes:
                base.pop(k, None)
            yield bucket, memoryview(db_boosts.serialize(base))


    def setdefault(self, key: bytes, default: bytes) -> bytes:
        """
        if key exists, return its value
        otherwise insert key with default and return default
        """
        try:
            return self.__getitem__(key)
        except KeyError:
            # only if not present, write it once
            self.__setitem__(key, default)
            return default


    def _flush_buffer(self) -> None:
        """
        Applies buffered sets and deletes to LMDB
        """
        if not (self._put_buffer or self._del_buffer or self._put_buckets or self._delete_buckets):
            return

        with self.env.begin(write=True, db=self._db, buffers=True) as txn:
            cursor = txn.cursor()
            if self.do_batch_writes:
                self._bucket_gets = cursor.getmulti(keys=self._put_buckets.keys())
                self._deletes_gets = cursor.getmulti(keys=self._delete_buckets.keys())
                cursor.putmulti(items=self._puts_gen_batched(), reserve=1)
            else:
                cursor.putmulti(items=self._put_buffer.items(), reserve=1)
                for key in self._del_buffer:
                    cursor.delete(key)
                    self.absent.add(key)

        # clear buffers
        self._put_buffer.clear()
        self._del_buffer.clear()
        self._put_buckets.clear()
        self._delete_buckets.clear()

    def __len__(self) -> int:
        """
        Returns number of entries in dict
        """
        with self.env.begin(db=self._db) as txn:
            return int(txn.stat(db=self._db)["entries"])

    def iterate(self, keys=True, values=False):
        """
        Iterator over keys and/or values. You have to flush your
        pending changes before iterating, if you are using
        # batching.
        Both keys and values will be returned in batched dicts.
        """
        if self.do_batch_writes:
          #  if self._buffer: raise Exception("Flush buffer before iterating")
            with self.env.begin(db=self._db, write=False) as txn:
                for bucket, raw in txn.cursor().iternext(keys=True, values=True):
                    base = self.fetched_buckets.get(bucket)
                    if base is None:
                        base = db_boosts.deserialize(raw)
                        self.fetched_buckets[bucket] = base
                    yield from base.items()
            return

        # non-batched
        with self.env.begin(db=self._db) as txn:
            cursor = txn.cursor().iternext(keys=keys, values=values)
            for elem in cursor:
                k, v = (elem if keys and values else
                        (elem[0], None) if keys else
                        (None, elem[1]) if values else
                        (None, None))
                if k in self._del_buffer:
                    continue
                if k in self._cache:
                    yield (k, self._cache[k]) if values else k
                    continue
                if values and k is not None:
                    self._cache[k] = v
                yield (k, v) if values else k

        # finally buffered non-batched puts
        if keys and values:
            yield from self._put_buffer.items()
        elif keys:
            yield from self._put_buffer.keys()
        else:
            yield from self._put_buffer.values()

    def __iter__(self):
        """
        Use iterate() for full control
        """
        raise NameError('Use method "iterate" instead')

    def __contains__(self, key: bytes) -> bool:
        """
        Checks existence of key in dict, considering buffers
        """
        if key in self._cache or key in self._put_buffer:
            return True
        if key in self.absent:
            return False

        if not self.do_batch_writes:
            raw = self.env.begin(db=self._db, write=False).get(key)
            if raw is None:
                self.absent.add(key)
                return False
            self._cache[key] = raw
            return True

        bucket = db_boosts.bucket(key, self.batch_size)
        if bucket in self._delete_buckets and key in self._delete_buckets[bucket]:
            return False
        if bucket in self._put_buckets and key in self._put_buckets[bucket]:
            return True

        if bucket not in self.fetched_buckets:
            raw = self.env.begin(db=self._db, write=False).get(bucket)
            if raw is None:
                self.absent.add(key)
                return False
            mapping = db_boosts.deserialize(raw)
            self.fetched_buckets[bucket] = mapping
        else:
            mapping = self.fetched_buckets[bucket]

        exists = key in mapping
        if exists:
            self._cache[key] = mapping[key]
        else:
            self.absent.add(key)
        return exists

    def __repr__(self) -> str:
        """
        Returns representation of StoredDict
        """
        return f"<StoredDict entries={len(self)}>"