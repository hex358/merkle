from nyada import *
from time import perf_counter
import xxhash

# Benchmark:
# 12_600_000 per second -
#

spread = lambda s: s % 50
shard = lambda s: xxhash.xxh32(s).intdigest() % 50



#scalable = ScalableStoredDict(name="hii", num_shards=50, buffer_size=0, variant_typed=False, sharding_call=shard, env_size_mb=30 )
scalable = StoredList(name="hii2", cache_on_set=False, batch_writes=256, max_length=128, constant_length=1)

if 0:
    j = 0
    t = perf_counter()
    for i in range(1000000):
        j = (bytes(scalable[i]))
    print((perf_counter() - t) / 1000000)
    print(j)
    quit()

encoded = (128*str(5)).encode("ascii")
for i in range(1000000):
    scalable.append(encoded)

#del scalable[b"5"]
t = perf_counter()


scalable.flush_buffer(threaded=0)
print(perf_counter() - t)

#print(bytes(scalable[-1]))

#print(scalable["5"])