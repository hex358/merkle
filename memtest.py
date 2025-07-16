from pymemcache.client.base import Client

# configure your memcached client
client = Client(('127.0.0.1', 11211), default_noreply=1, no_delay=1)

class MemStoredList:
    def __init__(self, name: str, buffer_size: int = 0):
        if not name:
            raise ValueError("name must be a non-empty string")
        self._key = f"list:{name}"
        self._buf_limit = buffer_size
        self._buffer = []

        # load persisted list (memcached pickles Python objects)
        persisted = client.get(self._key)
#        print(persisted[:15])
        self._persisted = list(persisted) if isinstance(persisted, list) else []

    def append(self, value: str) -> None:
        """Buffer in Python; flush only when buffer is full."""
        self._buffer.append(str(value))
        client.set(self._key, value)
        if self._buf_limit and len(self._buffer) >= self._buf_limit:
            self.flush()

    def flush(self) -> None:
        """Flush buffer: extend persisted list, write whole list back."""
        if not self._buffer:
            return
        # extend our in-memory copy
       # self._persisted.extend(self._buffer)
        # set the entire list back into memcached
        #client.set(self._key, self._buffer)
      #  self._buffer.clear()

    def __len__(self) -> int:
        return len(self._persisted) + len(self._buffer)

    def __getitem__(self, index: int) -> str:
        total = len(self)
        if index < 0:
            index += total
        if not (0 <= index < total):
            raise IndexError("list index out of range")

        if index < len(self._persisted):
            return self._persisted[index]
        else:
            return self._buffer[index - len(self._persisted)]

    def __setitem__(self, index: int, value: str) -> None:
        total = len(self)
        if index < 0:
            index += total
        if not (0 <= index < total):
            raise IndexError("list assignment index out of range")

        if index < len(self._persisted):
            # update persisted element and write back
            self._persisted[index] = str(value)
            client.set(self._key, self._persisted)
        else:
            # update in-buffer element
            self._buffer[index - len(self._persisted)] = str(value)

    def __iter__(self):
        # yield persisted elements, then buffered ones
        for v in self._persisted:
            yield v
        for v in self._buffer:
            yield v

    def __repr__(self):
        return f"<MemStoredList key={self._key!r} len={len(self)}>"




from time import perf_counter
if __name__ == "__main__":

    a = list( range(10000000))
    print("read..")
    t = perf_counter()
    lst = MemStoredList("e44", buffer_size=100000)
   # print(len(lst))
    print(perf_counter() - t)

    print("begin..")
    t = perf_counter()
    for i in range(1000000):
        lst.append("hi")
    # flush any remaining buffered items
    #lst.flush()
    print(perf_counter() - t)
    #print(len(lst))
