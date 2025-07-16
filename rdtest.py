import redis
client = redis.Redis.from_url("redis://localhost:6379/0")

import redis

class RedisStoredList:
    def __init__(self,
                 name: str,
                 redis_url: str = "redis://localhost:6379/0",
                 buffer_size: int = 0):
        if not name:
            raise ValueError("name must be a non-empty string")
        self._client = redis.Redis.from_url(redis_url)
        self._key = f"list:{name}"
        self._buf_limit = buffer_size
        self._buffer = []

        # cache the current on-disk length
        self._persisted_len = self._client.llen(self._key)

    def append(self, value: str) -> None:
        """Buffer in Python; flush only when buffer is full."""
        self._buffer.append(str(value))
        if self._buf_limit and len(self._buffer) >= self._buf_limit:
            self.flush()

    def flush(self) -> None:
        """One RPUSH pipeline call, then update our cached length."""
        if not self._buffer:
            return
        pipe = self._client.pipeline()
        # batch all buffered values in one round-trip
        pipe.rpush(self._key, *self._buffer)
        pipe.execute()

        # reflect that new data is now persisted
        self._persisted_len += len(self._buffer)
        self._buffer.clear()

    def __len__(self) -> int:
        # no LLEN call here!
        return self._persisted_len + len(self._buffer)

    def __getitem__(self, index: int) -> str:
        total = len(self)
        if index < 0:
            index += total
        if not (0 <= index < total):
            raise IndexError("list index out of range")

        if index >= self._persisted_len:
            # still in our local buffer
            return self._buffer[index - self._persisted_len]

        # only one round-trip for LINDEX
        val = self._client.lindex(self._key, index)
        if val is None:
            raise IndexError("list index out of range")
        return val.decode()

    def __setitem__(self, index: int, value: str) -> None:
        total = len(self)
        if index < 0:
            index += total
        if not (0 <= index < total):
            raise IndexError("list assignment index out of range")

        if index >= self._persisted_len:
            # in-buffer
            self._buffer[index - self._persisted_len] = str(value)
        else:
            # one round-trip for LSET
            self._client.lset(self._key, index, str(value))

    def __iter__(self):
        # LRANGE is one shot, better than a cursor loop
        for val in self._client.lrange(self._key, 0, -1):
            yield val.decode()
        yield from self._buffer

    def __repr__(self):
        return f"<RedisStoredList key={self._key!r} len={len(self)}>"


from time import perf_counter
if __name__ == "__main__":

    a = list( range(90000))
    lst = RedisStoredList("mylist", buffer_size=0)
    t = perf_counter()
    lst._buffer = a
    # flush any remaining buffered items
    lst.flush()
    print(perf_counter() - t)