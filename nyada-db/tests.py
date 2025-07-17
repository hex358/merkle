from nyada import *
from time import perf_counter
import xxhash

spread = lambda s: s % 50
shard = lambda s: xxhash.xxh32(s).intdigest() % 50



#scalable = ScalableStoredDict(name="hii", num_shards=50, buffer_size=0, variant_typed=False, sharding_call=shard, env_size_mb=30 )
scalable = StoredList(name="hii2", cache_on_set=False, batch_writes=4, max_length=19)

encoded = "6".encode("ascii")
for i in range(9):
    scalable.append(encoded)
scalable.append("8".encode("ascii"))

#del scalable[b"5"]
t = perf_counter()


scalable.flush_buffer(threaded=0)
print(perf_counter() - t)

scalable = StoredList(name="hii2", cache_on_set=False, batch_writes=4, max_length=19)
print(bytes(scalable[-1]))

#print(scalable["5"])