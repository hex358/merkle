from sanic import text, Sanic, html, file
from sanic import json as sanic_json
from sanic.exceptions import NotFound, InvalidUsage
import json
import mmr
from functools import wraps
import traceback
import web.app_router as router

def contract(structure: dict):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request = args[0]
            received = request.json or {}
            # basic structure valida tion
            s_error = sanic_json({"status": "ERR", "message": f"Incorrect data format: expected {structure}"})
            for i in received:
                if i not in structure: return s_error
                is_tuple = isinstance(structure[i], tuple)
                length = structure[i][0] if is_tuple else structure[i]
                if length and length != len(received[i]): return s_error
                if is_tuple and not isinstance(received[i], structure[i][1]): return s_error

            for i in structure:
                if not (i in received):
                    return s_error
            try:
                result = await func(*args, **kwargs)
            except Exception as e:
                print("".join(traceback.format_exception(e)))
                return sanic_json({"status": "ERR", "message": "Internal server error"})
            return result
        return wrapper
    return decorator

app = Sanic("test")


class Services:
    @classmethod
    def try_login(cls, token: str) -> bool:
        return True

    @classmethod
    def register(cls, name: str, metadata: dict) -> str:
        return "1234567"

    @classmethod
    def update(cls, token: str, metadata: dict) -> bool:
        return True

    @classmethod
    def get_metadata(cls, name: str) -> dict:
        return {}

    @classmethod
    def service_exists(cls, name: str) -> bool:
        return True

@app.post("/register_service")
@contract({"metadata": (0, dict), "service_name": (0, str)})
async def register_service(request):
    registered = Services.register(request.json["service_name"], request.json["metadata"])
    if registered:
        return sanic_json({"status": "OK", "service_token": str(registered), "message": ""})
    else:
        return sanic_json({"status": "ERR", "message": "service_name occupied"})

@app.post("/update_service")
@contract({"token": (0, str), "metadata": (0, dict)})
async def update_service(request):
    ok = Services.update(request.json["token"], request.json["metadata"])
    if ok:
        return sanic_json({"status": "OK", "message": ""})
    else:
        return sanic_json({"status": "ERR", "message": "Invalid token"})


@app.post("/add_blob")
@contract({"token": (0, str), "blob_hash": (16, str)})
async def add_blob(request):
    if not Services.try_login(request.json["token"]):
        return sanic_json({"status": "ERR", "message": "Invalid token"})
    mmr.add(bytes.fromhex(request.json["blob_hash"]))
    return sanic_json({"status": "OK", "message": ""})

@app.post("/check_blob")
@contract({"token": (0, str), "blob_hash": (16, str)})
async def check_blob(request):
    if not Services.try_login(request.json["token"]):
        return sanic_json({"status": "ERR", "message": "Invalid token"})
    return sanic_json({"status": "OK", "message": "", "bundle": mmr.server_check(bytes.fromhex(request.json["blob_hash"]))})

@app.post("/get_service")
@contract({"name": (0, str)})
async def get_service(request):
    if not Services.service_exists(request.json["name"]):
        return sanic_json({"status": "ERR", "message": "Service doesn't exist"})
    return sanic_json({"status": "OK", "message": "", "metadata": Services.get_metadata(request.json["name"])})

@app.route("/")
async def index(_):
    html_file = router.read("index")
    return html(html_file)

@app.route('/styles.css')
async def serve_css(request):
    return text(router.read("index", "css"), content_type="text/css")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

# sanic api.app --dev