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
            {"name": "body", "type": "string", "tokenize": "ngram", "infix": True}
        ]
    }
    try:
        client.collections[collection].retrieve()
    except Exception:
        client.collections.create(schema)

def add(key: str, body: str):
    return client.collections[collection].documents.upsert({"key": key, "body": body})

def register_result(res: str):
    res = res.replace(".", "_")
    return add(res, res)

def find_results(res: str):
    res = res.replace(".", "_")
    return search(res, 250)

def remove_result(who: str):
    who = who.replace(".", "_")
    return remove(who)

def search_all_parallel(
    query: str,
    num_typos: int = 2,
    per_page: int = 250,
    max_workers: int = 8,
    filter_by: str | None = None,
) -> list[dict]:
    """
    Fetch *all* matching documents via parallel page fetches.
    - Preserves ranking/page order.
    - Returns a list of `document` dicts.
    """
    # 1) Prime request to learn total hits & capture page 1
    first = _search_page(1, query, num_typos, per_page, filter_by)
    hits_page1 = first.get("hits", [])
    found = first.get("found", len(hits_page1))
    if found <= len(hits_page1):
        return [h["document"] for h in hits_page1]

    total_pages = math.ceil(found / per_page)
    results_by_page: dict[int, list] = {1: hits_page1}

    pages = list(range(2, total_pages + 1))
    if not pages:
        return [h["document"] for h in hits_page1]

    workers = min(max_workers, len(pages))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        fut2page = {
            ex.submit(_search_page, p, query, num_typos, per_page, filter_by): p
            for p in pages
        }
        for fut in as_completed(fut2page):
            p = fut2page[fut]
            try:
                res = fut.result()
                results_by_page[p] = res.get("hits", [])
            except Exception:
                try:
                    res = _search_page(p, query, num_typos, per_page, filter_by)
                    results_by_page[p] = res.get("hits", [])
                except Exception:
                    results_by_page[p] = []

    out: list[dict] = []
    for p in range(1, total_pages + 1):
        out.extend(h["document"] for h in results_by_page.get(p, []))
    return out

def _search_page(page: int, query: str, num_typos: int, per_page: int, filter_by: str | None):
    payload = {
        "q": query,
        "query_by": "body",
        "num_typos": num_typos,
        "per_page": per_page,
        "page": page,
    }
    if filter_by:
        payload["filter_by"] = filter_by
    return client.collections[collection].documents.search(payload)


def search(query: str, limit: int = 5, num_typos: int = 2):
    query = query.replace(".", "_")
    page = 1
    results = []
    while True:
        res = client.collections[collection].documents.search({
            "q": query,
            "query_by": "body",
            "num_typos": num_typos,
            "per_page": 250,
            "page": page
        })
        hits = res.get("hits", [])
        if not hits:
            break
        results.extend(hit["document"] for hit in hits)
        page += 1
    return results

def remove(key: str):
    # Fast delete by indexed key
    return client.collections[collection].documents.delete({"filter_by": f"key:={key}"})

import json
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
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
