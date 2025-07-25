from interface import *
from time import perf_counter


check = StoredList(name=b"dict", batching_config=BatchingConfig(batch_size=512,constant_length=1, max_item_length=1)
                   )


for i in range(1000000):
    check.append(b'rrrrrr')

t = perf_counter()
check.flush_buffer()
print(perf_counter() - t)


t = perf_counter()
for i in check:
    pass

print(perf_counter() - t)