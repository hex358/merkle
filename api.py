# ----------------- imports -----------------
import os, json, base64, hashlib, secrets, traceback
from functools import wraps

from sanic import Sanic, html, text
from sanic import json as sanic_json
from sanic.exceptions import NotFound, InvalidUsage

# project modules
import web.app_router as router
import database.interface as interface
import mmr

# ----------------- constants -----------------
_PBKDF2_ITERS = 100_000
app = Sanic("test")
cached_services: dict = {}

# ----------------- utils -----------------
def hash_password(password: str) -> str:
    try:
        salt = os.urandom(16)
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, _PBKDF2_ITERS)
        return f"{base64.urlsafe_b64encode(salt).decode()}$" \
               f"{base64.urlsafe_b64encode(dk).decode()}"
    except Exception:
        raise BaseException("Invalid Input Data")

def verify_password(password: str, stored: str) -> bool:
    try:
        salt_b64, hash_b64 = stored.split("$", 1)
        salt = base64.urlsafe_b64decode(salt_b64)
        expected_dk = base64.urlsafe_b64decode(hash_b64)
    except Exception:
        return False
    new_dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, _PBKDF2_ITERS)
    return secrets.compare_digest(new_dk, expected_dk)

def contract(structure: dict):
    """Decorator: validates request.json matches structure spec."""
    def decorator(func):
        @wraps(func)
        async def wrapper(request, *args, **kwargs):
            received = request.json or {}
            err = sanic_json({"status": "ERR", "message": f"Incorrect data format: expected {structure}"})

            for k, spec in structure.items():
                if k not in received: return err
                is_tuple = isinstance(spec, tuple)
                length, typ = (spec if is_tuple else (spec, None))
                if length and len(received[k]) != length: return err
                if typ and not isinstance(received[k], typ): return err
            try:
                return await func(request, *args, **kwargs)
            except Exception as e:
                print("".join(traceback.format_exception(e)))
                return sanic_json({"status": "ERR", "message": "Internal server error"})
        return wrapper
    return decorator

# ----------------- services -----------------
class Services:
    @classmethod
    def try_login(cls, name: str, token: str) -> bool:
        return True  # placeholder

    @classmethod
    def register(cls, name: str, metadata: dict) -> str:
        if not name in cached_services:
            cached_services[name] = mmr.MerkleService(name)
        return "1234567"

    @classmethod
    def update(cls, token: str, metadata: dict) -> bool:
        for k,v in metadata.items():
            metadata[k.encode()] = v.encode()
        mmr.set_service("gg", metadata)
        return True  # placeholder

    @classmethod
    def get_metadata(cls, name: str) -> dict:
        stored = mmr.get_meta(name.encode())
        for k,v in stored.items():
            stored[k.decode()] = v.decode()
        if stored is None:
            raise NameError(f"Service {name} doesn't exist")
        return stored

    @classmethod
    def service_exists(cls, name: str) -> bool:
        return mmr.has_service(name)

def get_service_obj(name: str):
    if name not in cached_services:
        cached_services[name] = mmr.MerkleService(name)
    return cached_services[name]

# ----------------- endpoints -----------------
@app.post("/register_service")
@contract({"metadata": (0, dict), "service_name": (0, str)})
async def register_service(request):
    name = request.json["service_name"]
    if Services.service_exists(name):
        return sanic_json({"status": "ERR", "message": "Service already exists"})
    token = Services.register(name, request.json["metadata"])
    return sanic_json({"status": "OK", "service_token": token, "message": ""})

@app.post("/update_service")
@contract({"token": (0, str), "service_name": (0, str), "metadata": (0, dict)})
async def update_service(request):
    if not Services.try_login(request.json["service_name"], request.json["token"]):
        return sanic_json({"status": "ERR", "message": "Invalid service"})
    return sanic_json({"status": "OK" if Services.update(request.json["token"], request.json["metadata"]) else "ERR"})

@app.get("/list_services")
async def list_services(_):
    result = []
    for name in ["gg"]:
        result.append({
            "service_name": name,
            "metadata": Services.get_metadata(name),
            "token": "hidden"  # you can return token or not, depending on policy
        })
    return sanic_json({"status": "OK", "services": result})


@app.post("/add_blob")
@contract({"token": (0, str), "service_name": (0, str), "blob_hash": (16, str)})
async def add_blob(request):
    if not Services.try_login(request.json["service_name"], request.json["token"]):
        return sanic_json({"status": "ERR", "message": "Invalid token"})
    merkle_service = get_service_obj(request.json["service_name"])
    merkle_service.add(bytes.fromhex(request.json["blob_hash"]))
    merkle_service.flush()
    return sanic_json({"status": "OK", "message": ""})

@app.post("/update_token")
@contract({"username": (0, str), "password": (0, str)})
async def update_token(request):
    # Returns newly generated token. Feed the requests any username/pass pair, assume it will work.
    # I will implement this on backend later, act as it returns {"token" : ...}
    pass

@app.get("/get_token")
@contract({"username": (0, str), "password": (0, str)})
async def get_token(request):
    # Returns generated token. Feed the requests any username/pass pair, assume it will work.
    # I will implement this on backend later, act as it returns {"token" : ...}
    pass

@app.post("/check_blob")
@contract({"service_name": (0, str), "blob_hash": (16, str)})
async def check_blob(request):
    name = request.json["service_name"]
    if not Services.service_exists(name):
        return sanic_json({"status": "ERR", "message": "Service doesn't exist"})
    bundle = get_service_obj(name).server_check(bytes.fromhex(request.json["blob_hash"]))
    return sanic_json({"status": "OK", "bundle": bundle, "message": ""})

@app.post("/get_service")
@contract({"service_name": (0, str)})
async def get_service(request):
    name = request.json["service_name"]
    if not Services.service_exists(name):
        return sanic_json({"status": "ERR", "message": "Service doesn't exist"})
    print(Services.get_metadata(name))
    return sanic_json({"status": "OK", "metadata": Services.get_metadata(name), "message": ""})

@app.route("/")
async def index(_): return html(router.read("index"))

@app.route("/signup")
async def signup(_): return html(router.read("signup"))

@app.route("/dashboard")
async def dashboard(_): return html(router.read("dashboard"))

@app.route("/login")
async def login(_): return html(router.read("login"))

from sanic.response import file
from pathlib import Path
@app.route("/<filepath:path>")
async def serve_asset(_, filepath: str):
    path = Path(filepath)

    if len(path.parts) < 2 or not path.suffix:
        raise NotFound("Invalid asset path")

    ext = path.suffix.lstrip(".").lower()
    page = path.parts[0]
    asset = "/".join(path.parts[1:])

    if ext == "css":
        return text(router.read(page, "css", asset), content_type="text/css")

    elif ext == "js":
        return text(router.read(page, "js", asset), content_type="application/javascript")

    elif ext in ("png", "jpg", "jpeg", "gif", "svg", "webp", "ico"):
        return await file(f"web/{page}/{asset}")

    else:
        raise NotFound(f"Unknown asset type: {ext}")


# ----------------- run -----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
