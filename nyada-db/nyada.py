# Nyada 1.0.0
# Speed benchmark

import os
import struct
import lmdb
from threading import Thread
import db_boosts

# ensure database directory exists
if not os.path.isdir("db"):
    # create database directory if it does not exist
    os.mkdir("db")


def open_environment(name: str,
                     size_mb: float,
                     lock_safe: bool = True,
                     max_variables: int = 1024):
    """
    Opens LMDB environment with given parameters
    """
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


env = open_environment("db", 4096, False)  # initialize global env (1 GiB)


class StoredReference:
    references = {}

    @staticmethod
    def register(instance: "StoredObject"):
        """
        Registers instance in references dictionary
        """
        StoredReference.references[instance.name] = instance

    @staticmethod
    def from_reference(name: bytes) -> "StoredObject":
        """
        Retrieves stored object by name
        """
        return StoredReference.references[name]

    def __init__(self, name: bytes):
        """
        Initializes stored reference with name
        """
        self.name = name


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
            return b'r' + value.name
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
            return StoredReference.from_reference(payload)
        case _:
            # unknown prefix raises error
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
                 env=env,                        # - Custom environment. Global one as default.
                 cache_on_set: bool = False,     # - Cache on __setitem__ calls. Will slow down
                                                 # the SETs, and speed up the GETs.
                 max_item_length: int = 0,       # - Max length of 1 item.
                 batch_size: int = 0,            # - Batch size. Will boost the perfomance by
                                                 # storing multiple values under same key.
                                                 # In StoredDict, represents the number of
                                                 # buckets.
                 constant_length: bool = False   # - Speeds up all the operations, since the
                                                 # need for constructing batch header each time
                                                 # goes away. This property has no effect if
                                                 # batching is disabled.
                 ):
        """
        Initializes StoredObject with configuration parameters
        """
        assert name, "Name must be a non-empty bytes"
        self.env = env
        self.cache_on_set = cache_on_set
        self.do_batch_writes = bool(batch_size)
        self.batch_size = batch_size
        self.constant_length = constant_length
        self.max_length = max_item_length

        # statistics DB
        self.stat = env.open_db(name + b"__stat", create=True)
        self.stat_cache = {}
        self.flushing_thread = None

        # header layout
        self.byte_length = 8
        self.HEADER_SLOT_COUNT = batch_size + 1
        self.HEADER_BYTE_COUNT = self.HEADER_SLOT_COUNT * self.byte_length

        # struct pack/unpack helpers
        self.struct_pack_into = struct.Struct(">Q").pack_into
        self.struct_unpack_from = struct.Struct(">Q").unpack_from

        # for batch constant-length operations
        self.typecode = "Q"
        self.pack_format = f"={self.batch_size+1}Q"

        # map initial statistics
        self.map_stat()

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
        Hook for subclasses to flush their buffers
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
    """
    LMDB-backed list with in-memory buffer and cache (append-only)
    """
    stat_fields = {b"type": b"1"}
    int_pack = staticmethod(lambda idx: struct.pack("=Q", idx))

    def __init__(self, **kwargs):
        """
        Initializes StoredList with buffering and cache
        """
        super().__init__(**kwargs)
        name = kwargs["name"]
        self._db = env.open_db(name, create=True)
        self._buffer = []  # pending appends
        self._cache = {}   # read cache
        self._batched_writes = {}
        self._index_buffer = {}
        self.buffer_batches = {}
        self.fetched_blobs = {}

        if not self.do_batch_writes:
            with self.env.begin(db=self._db) as txn:
                self._persisted_len = int(txn.stat(db=self._db)["entries"])
        else:
            raw = self.get_stat(b"length")
            self._persisted_len = int(raw.decode())

        # placeholders for partial pages
        self.old_header = bytearray(self.HEADER_BYTE_COUNT)
        self.old_body = bytearray()

    def _map_stat(self, txn):
        """
        Maps persisted length stat when batching
        """
        if self.do_batch_writes:
            raw = self.get_stat(b"length")
            self._persisted_len = int(raw.decode())

    def _puts_gen_batched(self):
        """
        Generator for batched page writes
        """
        bs = self.batch_size
        bl = self.byte_length

        page = self._persisted_len // bs
        offset = self._persisted_len % bs

        header_arr = array('Q', [0] * (bs + 1))
        body_chunks = []

        if self.old_header:
            # restore partial page header/body
            header_arr.frombytes(self.old_header)
            body_chunks = [bytes(self.old_body)]
            running = header_arr[offset]
        else:
            running = 0
            if offset:
                with self.env.begin(db=self._db, write=False, buffers=True) as txn:
                    raw = txn.get(StoredList.int_pack(page)) or b''
                hdr = raw[: bl * (bs + 1)]
                body = raw[bl * (bs + 1): bl * (bs + 1) + offset * bl]
                header_arr.frombytes(hdr)
                body_chunks = [body]
                running = header_arr[offset]

        idx = self._persisted_len
        slot = offset

        for v in self._buffer:
            header_arr[slot] = running
            body_chunks.append(v)
            running += len(v)
            slot += 1
            idx += 1

            if slot == bs:
                # flush full page
                header_arr[bs] = running
                data = header_arr.tobytes() + b''.join(body_chunks)
                yield (StoredList.int_pack(page), memoryview(data))
                page += 1
                slot = 0
                running = 0
                body_chunks.clear()

        if slot != offset:
            # flush partial page
            header_arr[slot] = running
            data = header_arr.tobytes() + b''.join(body_chunks)
            yield (StoredList.int_pack(page), memoryview(data))

        # save for next flush
        self.old_header = bytearray(header_arr.tobytes())
        self.old_body = bytearray().join(body_chunks)

    def _puts_gen_single(self):
        """
        Generator for single writes (no batching)
        """
        idx = self._persisted_len
        for v in self._buffer:
            yield (StoredList.int_pack(idx), v)
            idx += 1

    def _puts_gen_constant(self):
        """
        Generator for constant-length batched writes
        """
        bs, ml = self.batch_size, self.max_length
        start_page = self._persisted_len // bs
        start_off = self._persisted_len % bs

        if self.old_body:
            old = bytes(self.old_body)
        elif start_off:
            with self.env.begin(db=self._db, write=False) as txn:
                raw = txn.get(StoredList.int_pack(start_page)) or b''
            old = raw[: start_off * ml]
        else:
            old = b''

        buf = self._buffer
        total = start_off + len(buf)
        pages = (total + bs - 1) // bs
        if total == start_off and not buf:
            return

        for p in range(pages):
            page = start_page + p
            gstart = p * bs
            gend = min(gstart + bs, total)
            bstart = max(0, gstart - start_off)
            bend = gend - start_off

            if p == 0 and start_off:
                chunk = old + b"".join(buf[:bend])
            else:
                chunk = b"".join(buf[bstart:bend])
            yield StoredList.int_pack(page), memoryview(chunk)

    def _sets_gen(self):
        """
        Generator for in-place index updates
        """
        for idx, val in self._index_buffer.items():
            yield (StoredList.int_pack(idx), val)

    def _sets_gen_batched(self):
        """
        Generator for batched in-place updates
        """
        bs = self.batch_size
        hdr = self.HEADER_BYTE_COUNT
        txn = self.env.begin(db=self._db, write=False, buffers=True)

        for page, blob in self.get_results:
            assigns = self._batched_writes[page]

            if self.constant_length:
                # overwrite fixed-size slots
                chunk = bytearray(blob)
                for slot, v in assigns.items():
                    if len(v) != self.max_length:
                        raise ValueError(
                            f"slot {slot}: expected length {self.max_length}, got {len(v)}"
                        )
                    start = slot * self.max_length
                    chunk[start:start + self.max_length] = v
            else:
                # rebuild header and body for variable lengths
                offsets = struct.unpack(self.pack_format, blob[:hdr])
                body_chunks = []
                for slot in range(bs):
                    if slot in assigns:
                        body_chunks.append(assigns[slot])
                    else:
                        s, e = offsets[slot], offsets[slot+1]
                        start = hdr + s
                        end = hdr + e
                        body_chunks.append(blob[start:end])

                new_hdr = array('Q', [0] * (bs + 1))
                run = 0
                for i, chunk_bytes in enumerate(body_chunks):
                    new_hdr[i] = run
                    run += len(chunk_bytes)
                new_hdr[bs] = run
                chunk = new_hdr.tobytes() + b''.join(body_chunks)

            yield (page, memoryview(chunk))

    def _flush_buffer(self) -> None:
        """
        Writes buffered items to LMDB
        """
        if not self._buffer and not self._batched_writes:
            return

        total = self._persisted_len + len(self._buffer)
        with self.env.begin(write=True, db=self._db, buffers=True) as txn:
            cursor = txn.cursor()
            puts = self._puts_gen_single if not self.do_batch_writes else (
                self._puts_gen_constant if self.constant_length else self._puts_gen_batched
            )
            cursor.putmulti(append=True, items=puts())

            sets = self._sets_gen if not self.do_batch_writes else self._sets_gen_batched
            self.get_results = cursor.getmulti(self._batched_writes.keys())
            cursor.putmulti(items=sets())

        if self.do_batch_writes:
            self.write_stat(b"length", str(total).encode("ascii"))
        self._persisted_len = total
        self._batched_writes.clear()
        self._buffer.clear()

    def __len__(self) -> int:
        """
        Returns total number of items, including buffered
        """
        return self._persisted_len + len(self._buffer)

    def __getitem__(self, index: int) -> bytes:
        """
        Retrieves item by index, supporting negative and cached reads
        """
        length = len(self)
        if index < 0:
            # adjust negative index
            index += length
        if not 0 <= index < length:
            raise IndexError("index out of range")

        if index in self._cache:
            # return from read cache
            return self._cache[index]
        if index >= self._persisted_len:
            # return from in-memory buffer
            return self._buffer[index - self._persisted_len]

        if self.do_batch_writes:
            # compute page and slot for batched storage
            page, slot = divmod(index, self.batch_size)
            key = StoredList.int_pack(page)
        else:
            key = StoredList.int_pack(index)

        if not self.do_batch_writes:
            with self.env.begin(db=self._db, write=False, buffers=True) as txn:
                blob = txn.get(key)
        else:
            # fetch blob once per page
            if key not in self.fetched_blobs:
                with self.env.begin(db=self._db, write=False, buffers=True) as txn:
                    blob = txn.get(key)
                self.fetched_blobs[key] = blob
            else:
                blob = self.fetched_blobs[key]

        if blob is None:
            raise IndexError(f"{index} out of range")

        if not self.do_batch_writes:
            value = blob
        elif self.constant_length:
            # fixed-slot slicing
            start = slot * self.max_length
            end = start + self.max_length
            value = blob[start:end]
        else:
            # parse header for offsets
            hdr = blob[:self.HEADER_BYTE_COUNT]
            offsets = struct.unpack(self.pack_format, hdr)
            start, end = offsets[slot], offsets[slot+1]
            body_off = self.HEADER_BYTE_COUNT
            value = blob[body_off + start: body_off + end]

        if self.cache_on_set:
            # cache read value
            self._cache[index] = value
        return value

    def __setitem__(self, index: int, value: bytes) -> None:
        """
        Updates or buffers value at given index
        """
        length = len(self)
        if index < 0:
            # adjust negative index
            index += length
        if not 0 <= index < length:
            raise IndexError("assignment index out of range")

        if index >= self._persisted_len:
            # update in-memory buffer for new items
            self._buffer[index - self._persisted_len] = value
        else:
            if self.do_batch_writes:
                page, slot = divmod(index, self.batch_size)
                key = StoredList.int_pack(page)
                # collect batched updates
                if not key in self._batched_writes: self._batched_writes[key] = {slot: value}
                else: self._batched_writes[key][slot] = value
            else:
                # index-based buffer for single updates
                self._index_buffer[index] = value

        if self.cache_on_set:
            # update read cache
            self._cache[index] = value

    def _default_iter(self):
        """
        Iterator for non-batched storage
        """
        cursor = env.begin(db=self._db, write=False).cursor().iternext(values=True)
        idx_cursor = 0
        for i in range(len(self)):
            if i in self._cache:
                yield self._cache[i]
            else:
                # advance cursor to correct position
                steps = i - idx_cursor
                if steps != 1 and idx_cursor > 0:
                    for _ in range(steps):
                        idx_cursor += 1
                        _ = next(cursor)
                else:
                    idx_cursor += 1
                    _ = next(cursor)
                self._cache[i] = _
                yield _

    def _batch_iter(self):
        """
        Iterator for batched storage
        """
        cursor = env.begin(db=self._db, write=False).cursor().iternext(values=True)
        last_pos = 0
        blob = None

        for i in range(len(self)):
            if i in self._cache:
                yield self._cache[i]
                continue

            slot = i % self.batch_size
            if slot == 0:
                page = i // self.batch_size
                key = StoredList.int_pack(page)

                if key in self.fetched_blobs:
                    blob = self.fetched_blobs[key]
                else:
                    # advance cursor to page
                    steps = page - last_pos
                    if steps != 1 and last_pos > 0:
                        for _ in range(steps):
                            last_pos += 1
                            blob = next(cursor)
                    else:
                        last_pos += 1
                        blob = next(cursor)
                    self.fetched_blobs[key] = blob

                if not self.constant_length:
                    hdr = blob[:self.HEADER_BYTE_COUNT]
                    offsets = struct.unpack(self.pack_format, hdr)

            if self.constant_length:
                # optimized with promise that length is constant
                start = slot * self.max_length
                end = start + self.max_length
                val = blob[start:end]
            else:
                # otherwise, fetch with variable length
                start, end = offsets[slot], offsets[slot+1]
                val = blob[self.HEADER_BYTE_COUNT + start: self.HEADER_BYTE_COUNT + end]

            self._cache[i] = val
            yield val

    def __iter__(self):
        """
        Iterates over persisted and buffered items
        """
        if self.do_batch_writes:
            yield from self._batch_iter()
        else:
            yield from self._default_iter()
        yield from self._buffer

    def __repr__(self) -> str:
        """
        Returns representation of StoredList
        """
        return f"<StoredList len={len(self)}>"

class BucketOverlay:
    __slots__ = ("base", "puts", "dirty_full", "loaded")
    def __init__(self):
        self.base = None         # dict/None
        self.puts = {}           # dict[key] = value/TOMBSTONE
        self.dirty_full = False  # True->write fully
        self.loaded = False      # загружен ли base из LMDB


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

    def __getitem__(self, key: bytes) -> bytes:
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

        bucket = db_boosts.bucket(key, self.batch_size)
        if self.do_batch_writes:
            # if writes are batched, combine puts
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
            yield bucket, db_boosts.serialize(base)

        # partial deletes
        for bucket, deletes in self._delete_buckets.items():
            base = self.fetched_buckets.get(bucket) or db_boosts.deserialize(self._deletes_gets[bucket])
            for k in deletes:
                base.pop(k, None)
            yield bucket, db_boosts.serialize(base)


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
                cursor.putmulti(items=self._puts_gen_batched())
            else:
                cursor.putmulti(items=self._put_buffer.items())
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
            self.flush_buffer(threaded=False)
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
