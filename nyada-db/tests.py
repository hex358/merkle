from nyada import *
from time import perf_counter


a = StoredList(name=b"gg", batch_writes=512, constant_length=False, max_length=1, buffer_size=0)


if 0:

    for i in range(1000000):
        a[i] = b"r"*16
    t = perf_counter()
    a.flush_buffer()
    print(perf_counter() - t)
    quit()


for i in range(1000000):
    a.append(b"t"*1)



t = perf_counter()
a.flush_buffer()
print(perf_counter() - t)