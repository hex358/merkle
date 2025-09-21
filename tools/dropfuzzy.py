import typesense

client = typesense.Client({
    "nodes": [{"host": "localhost", "port": "8108", "protocol": "http"}],
    "api_key": "xyz",
    "connection_timeout_seconds": 2
})

collection = "docs"


def drop_collection():
    try:
        return client.collections[collection].delete()
    except Exception as e:
        return {"error": str(e)}


def clear_collection():
    try:
        return client.collections[collection].documents.delete({"filter_by": "*"})
    except Exception as e:
        return {"error": str(e)}


drop_collection()
