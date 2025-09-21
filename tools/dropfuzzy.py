import typesense
import os

TYPESENSE_HOST = os.getenv("TYPESENSE_HOST", "localhost")  # default = localhost
TYPESENSE_PORT = os.getenv("TYPESENSE_PORT", "8108")
TYPESENSE_API_KEY = os.getenv("TYPESENSE_API_KEY", "xyz")

client = typesense.Client({
    "nodes": [{"host": TYPESENSE_HOST, "port": TYPESENSE_PORT, "protocol": "http"}],
    "api_key": TYPESENSE_API_KEY,
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
