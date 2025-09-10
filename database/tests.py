from interface import *
from time import perf_counter

a = StoredReference.from_reference(b"node_hashes")
#a = StoredDict(name=b"node_hashes", batching_config=BatchingConfig(512))
# for i in range(5):
# 	a[i.to_bytes()] = b"ff"
# a.flush_buffer()
for i in a.iterate():
	print(i)
#print(StoredReference.from_reference(b"node_hashes"))