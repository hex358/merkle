from interface import *

a = len("hello world")

Start(4096*2, False, 1024)
lst = StoredDict(name=b"list", batching_config=BatchingConfig(1024, True, a))

lst[b"hi"] = StoredReference(StoredDict())
