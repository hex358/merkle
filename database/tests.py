from interface import StoredList, StoredReference, encode_val, decode_val, StoredDict, BatchingConfig, Start
from time import perf_counter

Start(".db", 4096, False, 30000)

test_string = b"Hello, World!"
a = StoredList(name=b"list", batching_config=BatchingConfig(512, True, len(test_string)))

for i in range(1_000_000):
	a.append(test_string) # We can add raw bytes.

for i in range(5_000):
	nested_dict = StoredDict(name=str(i).encode("utf-8") + b"_nest") # Or store encoded references to objects.
	# Like nested dicts.
	a.append(encode_val(StoredReference(nested_dict)))

t = perf_counter()
a.flush_buffer()
print(perf_counter() - t) # 0.008 ~ 100 mil

