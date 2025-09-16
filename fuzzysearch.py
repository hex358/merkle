import typesense

client = typesense.Client({
    "nodes": [{"host": "localhost", "port": "8108", "protocol": "http"}],
    "api_key": "xyz",
    "connection_timeout_seconds": 2
})

collection = "docs"

def ensure_collection():
    schema = {
        "name": collection,
        "fields": [
            {"name": "key", "type": "string", "facet": True},  # indexed for fast lookup
            {"name": "body", "type": "string"}
        ]
    }
    try:
        client.collections[collection].retrieve()
    except Exception:
        client.collections.create(schema)

def add(key: str, body: str):
    return client.collections[collection].documents.upsert({"key": key, "body": body})

def register_result(res: str):
    return add(res, res)

def find_results(res: str):
    return search(res, 250)

def remove_result(who: str):
    return remove(who)

def search(query: str, limit: int = 5, num_typos: int = 2):
    res = client.collections[collection].documents.search({
        "q": query,
        "query_by": "body",
        "num_typos": num_typos,
        "per_page": limit
    })
    return [hit["document"] for hit in res["hits"]]

def remove(key: str):
    # Fast delete by indexed key
    return client.collections[collection].documents.delete({"filter_by": f"key:={key}"})

import json
def iterate_all(from_page: int = 1, per_page: int = 250):
    """
    Iterate through all documents efficiently by paging.
    Uses documents.search with empty query (q="*").
    """
    page = from_page
    while True:
        res = client.collections[collection].documents.search({
            "q": "*",
            "query_by": "body",
            "per_page": per_page,
            "page": page
        })
        hits = res.get("hits", [])
        if not hits:
            break
        for hit in hits:
            yield hit["document"]
        page += 1

def total() -> int:
    return client.collections[collection].retrieve()["num_documents"]

# ensure_collection()
# from time import perf_counter
#
# itera = iterate_all()
# print(next(itera))
# t = perf_counter()
# print(next(itera))
# print(perf_counter() - t)
#


# if __name__ == "__main__":
#     ensure_collection()
#
#for i in range(100):
#    add("hello world", "hello world")
#     add("certumtree api dashboard", "certumtree api dashboard")
#
#     print(search("helo wrld"))   # fuzzy search
#     print(remove("hello world"))
#     print(search("cerfufmtree"))
