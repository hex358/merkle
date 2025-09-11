from interface import *

Start("my_database", 4096*2, False)

text = b"a"*500

stored = StoredList(name=b"list", batching_config=BatchingConfig(512, True, len(text)))

for i in range(1_000_000):
	stored.append(text)

from time import perf_counter
#
t = perf_counter()
stored.flush_buffer()
print(perf_counter() - t)

