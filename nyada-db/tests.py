from nyada import *
from time import perf_counter
import xxhash

spread = lambda s: s % 50
shard = lambda s: xxhash.xxh32(s).intdigest() % 50



#scalable = ScalableStoredDict(name="hii", num_shards=50, buffer_size=0, variant_typed=False, sharding_call=shard, env_size_mb=30 )
scalable = StoredList(name="hii2", variant_typed=0, cache_on_set=False, batch_writes=100)

encoded = "1_000_000".encode("ascii")
for i in range(1000000):
    scalable.append(encoded)
t = perf_counter()


scalable.flush_buffer(threaded=0)
print(perf_counter() - t)


#print(scalable["5"])