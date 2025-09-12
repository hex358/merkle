from interface import *

a = len("hello world")

Start(".db", 4096*2, False)
lst = StoredList(name=b"list", batching_config=BatchingConfig(1024, True, a))

for i in range(1_000_000):
	lst.append((b"elloh world"*1))

from time import perf_counter


t = perf_counter()
lst.flush_buffer()
print(perf_counter()-t)
