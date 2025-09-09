from interface import *
from time import perf_counter


check = StoredList(name=b"dict", batching_config=BatchingConfig(batch_size=512,constant_length=1, max_item_length=6)
                   )


import struct
for i in range(1000000):
    check.append(struct.pack(">d", 2.0))

t = perf_counter()
check.flush_buffer()
print(perf_counter() - t)


t = perf_counter()
for i in check:
    pass

print(perf_counter() - t)