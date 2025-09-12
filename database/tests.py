from interface import *

a = len("Hello warld!")

Start(".db", 4096, False)
lst = StoredList(name=b"list", batching_config=BatchingConfig(512, True, a))

for i in range(1_000_000):
	lst.append((b"H"*500))

from time import perf_counter

lst[5] = b"Hello world!"

t = perf_counter()
lst.flush_buffer()
print(perf_counter()-t)
