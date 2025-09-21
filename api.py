# ----------------- imports -----------------
import os, json, base64, hashlib, secrets, traceback
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from functools import wraps

from sanic import Sanic, html, text
from sanic import json as sanic_json
from sanic.exceptions import NotFound, InvalidUsage
import uuid
from sanic.response import file
from pathlib import Path
#from authlib.integrations.httpx_client import AsyncOAuth2Client

# project modules
import web.app_router as router
import database.interface as interface
import mmr
import fuzzysearch

# ----------------- constants -----------------
_PBKDF2_ITERS = 100_000
app = Sanic("certum")
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

def derive_key(password: str, salt: bytes, length: int = 32) -> bytes:
    """Derive symmetric key from password using PBKDF2-HMAC-SHA256."""
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, _PBKDF2_ITERS, dklen=length)

def encrypt_token(token: str, password: str) -> bytes:
    salt = os.urandom(16)
    key = derive_key(password, salt)
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ct = aesgcm.encrypt(nonce, token.encode(), None)
    # store all parts together
    return base64.b64encode(salt + nonce + ct)

def decrypt_token(enc: bytes, password: str) -> str:
    raw = base64.b64decode(enc)
    salt, nonce, ct = raw[:16], raw[16:28], raw[28:]
    key = derive_key(password, salt)
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, ct, None).decode()




# ----------------- services -----------------
stored_tokens = interface.StoredDict(name=b"tokens", cache_on_set=True)
hashed_tokens = interface.StoredDict(name=b"hashed_tokens", cache_on_set=True)
class Services:
    @classmethod
    def get_service(cls, name: str) -> bool:
        if name not in cached_services:
            cached_services[name] = mmr.MerkleService(name)
        return cached_services[name]

    @classmethod
    def check_token(cls, name: str, token: str) -> bool:
        try:
            return verify_password(token, hashed_tokens[name.encode()].decode())
        except Exception as e:
            return False

    @classmethod
    def register(cls, who: str, username: str, password: str) -> str:
        pathname = f"{username}.{who}"
        who_b = who.encode()
        user_b = username.encode()
        if not user_b in stored_user_passwords: return []

        if pathname not in cached_services:
            cached_services[pathname] = mmr.MerkleService(pathname)

        try:
            mmr.set_service(pathname, {})
        except Exception:
            pass

        # generate & store encrypted token
        token = str(uuid.uuid4())
        enc_token = encrypt_token(token, password)
        stored_tokens[pathname.encode()] = enc_token
        hashed_tokens[pathname.encode()] = hash_password(token).encode()

        # add service name ONCE to the user's list
        current = stored_user_services[user_b]
        items = [x for x in current.split(b":") if x]  # strip empties
        if who_b not in items:
            items.append(who_b)
        stored_user_services[user_b] = b":".join(items)

        # flush
        stored_user_services.flush_buffer()
        stored_tokens.flush_buffer()
        hashed_tokens.flush_buffer()

        fuzzysearch.register_result(pathname)

        return token


    @classmethod
    def gettoken(cls, who: str, user: str, key: str):
        try:
            return decrypt_token(stored_tokens[(user+"."+who).encode()], key)
        except:
            return ""

    @classmethod
    def update(cls, who: str, username: str, password: str, metadata: dict) -> bool:
        new_meta = {}
        for k,v in metadata.items():
            new_meta[k.encode()] = v.encode()

        mmr.set_service(username+ "." + who, new_meta)
        return True  # placeholder

    @classmethod
    def get_metadata(cls, name: str) -> dict:
        try:
            stored = mmr.get_meta(name.encode())
            if stored is None:
                raise NameError(f"Service {name} doesn't exist")
            new_stored = {}
            for k,v in stored.items():
                new_stored[k.decode()] = v.decode()
            return new_stored
        except:
            return {}

    @classmethod
    def service_exists(cls, name: str) -> bool:
        return mmr.has_service(name)

    @classmethod
    def my_service_list(cls, user: str) -> bool:
        res = []
        if not user.encode() in stored_user_services: return []
        for i in stored_user_services[user.encode()].split(b":"):
            if not i: continue
            res.append({"service_name": i.decode(), "metadata": cls.get_metadata((user.encode() + b"." + i).decode())})
        return res

def get_service_obj(name: str):
    if name not in cached_services:
        cached_services[name] = mmr.MerkleService(name)
    return cached_services[name]

def validate_password(username: str, password: str):
    try:
        return verify_password(password, stored_user_passwords[username.encode()].decode())
    except:
        return False

@app.post("/register_service")
@contract({"service_name": (0, str), "password": (0, str), "username": (0, str)})
async def register_service(request):
    name = request.json["service_name"]
    username = request.json["username"]
    password = request.json["password"]
    if Services.service_exists(username + "." + name) or ":" in name:
        return sanic_json({"status": "ERR", "message": "Service already exists"})
    token = Services.register(name, username, password)

    return sanic_json({"status": "OK", "service_token": token, "message": ""})

@app.post("/delete_service")
@contract({"service_name": (0, str), "password": (0, str), "username": (0, str)})
async def delete_service(request):
    username = request.json["username"]
    password = request.json["password"]
    service_name = request.json["service_name"]

    if not validate_password(username, password):
        return sanic_json({"status": "ERR", "message": "Wrong username or password"})

    pathname = f"{username}.{service_name}"
    user_b = username.encode()
    path_b = pathname.encode()

    if not Services.service_exists(pathname):
        return sanic_json({"status": "ERR", "message": "Service doesn't exist"})

    try:
        mmr.delete_service(pathname, cached_services)
    except Exception:
        pass

    if path_b in stored_tokens:
        del stored_tokens[path_b]
    if path_b in hashed_tokens:
        del hashed_tokens[path_b]

    if pathname in cached_services:
        del cached_services[pathname]

    if user_b in stored_user_services:
        current = stored_user_services[user_b]
        items = [x for x in current.split(b":") if x]
        new_items = [x for x in items if x.decode() != service_name]
        stored_user_services[user_b] = b":".join(new_items)

    stored_tokens.flush_buffer()
    hashed_tokens.flush_buffer()
    stored_user_services.flush_buffer()
    fuzzysearch.remove_result(pathname)

    return sanic_json({"status": "OK", "message": f"Service {service_name} deleted"})


@app.post("/get_root_hash")
@contract({"service_name": (0, str)})
async def get_root_hash(request):
    name = request.json["service_name"]
    if not Services.service_exists(name):
        return sanic_json({"status": "ERR", "message": "Service doesnt exist", "global_root": ""})

    byte = bytes(Services.get_service(name).get_global_root())
   # print(Services.get_service(name).get_global_root().hex())
    return sanic_json({"status": "OK", "global_root": byte.hex(), "message": ""})

@app.post("/update_service")
@contract({"username": (0, str), "password": (0, str), "service_name": (0, str), "metadata": (0, dict)})
async def update_service(request):
    if not Services.service_exists(request.json["username"] + "." + request.json["service_name"]):
        return sanic_json({"status": "ERR", "message": "Service doesn't exist"})
    if not validate_password(request.json["username"], request.json["password"]):
        return sanic_json({"status": "ERR", "message": "Incorrect password"})
    return sanic_json({"status": "OK" if Services.update(request.json["service_name"],
                                                         request.json["username"],
                                                         request.json["password"],
                                                         request.json["metadata"]) else "ERR"})

import cachetools
cached_search_results = cachetools.TTLCache(2**12, 60*5)


def clear_user_cache(username: str):
    for key in list(cached_search_results.keys()):
        if key[0] == username:
            del cached_search_results[key]

@app.middleware("response")
async def add_no_cache_headers(request, response):
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"



fuzzysearch.ensure_collection()
@app.post("/list_services")
async def list_services(request):
    username = request.json["username"]
    filter_text = request.json["filter"]
    page_id = int(request.json["page_id"])
    num_results = int(request.json["num_results"])

    cache_key = (username, filter_text)
    total = 0

    if filter_text:
        global cached_search_results
        if cache_key not in cached_search_results:
            results = fuzzysearch.search_all_parallel(filter_text, 3, 250, 4) or []
            cached_search_results[cache_key] = results
        all_results = cached_search_results[cache_key]

        start = (page_id - 1) * num_results
        end = start + num_results
        page_docs = all_results[start:end]
        total = len(all_results)

    else:
        page_docs = []
        total = fuzzysearch.total()
        gen = fuzzysearch.iterate_all(from_page=page_id, per_page=num_results)
        for i, doc in enumerate(gen):
            page_docs.append(doc)
            if i + 1 >= num_results:
                break

    services = []
    for d in page_docs:
        try:
            services.append({"service_name": d["body"]})
        except Exception:
            continue

    start = (page_id - 1) * num_results
    end = start + len(services)
    has_more = end < total

    return sanic_json({
        "status": "OK",
        "services": services,
        "total": total,
        "has_more": has_more,
        "next_page_id": page_id + 1 if has_more else None
    })



@app.post("/get_my_services")
@contract({"username": (0, str)})
async def get_my_services(request):
    return sanic_json({"status": "OK", "services": Services.my_service_list(request.json["username"])})


@app.post("/add_blob")
@contract({"token": (0, str), "service_name": (0, str), "blob_hash": (16, str)})
async def add_blob(request):
    if not Services.check_token(request.json["service_name"], request.json["token"]):
        return sanic_json({"status": "ERR", "message": "Invalid token"})
    merkle_service = get_service_obj(request.json["service_name"])
    merkle_service.add(bytes.fromhex(request.json["blob_hash"]))
    merkle_service.flush()
    return sanic_json({"status": "OK", "message": ""})


@app.post("/get_token")
@contract({"service_name": (0, str), "username": (0, str), "password": (0, str)})
async def get_token(request):
    tok = Services.gettoken(request.json["service_name"], request.json["username"], request.json["password"])
    if not tok:
        return sanic_json({"status": "ERR", "token": "", "message": ""})
    return sanic_json({"status": "OK", "token": tok, "message": ""})

@app.post("/user_login")
@contract({"username": (0, str), "password": (0, str)})
async def user_login(request):
    if not validate_password(request.json["username"], request.json["password"]):
        return sanic_json({"status": "ERR", "message": "Wrong username or password"})
    return sanic_json({"status": "OK", "message": ""})

stored_user_passwords = interface.StoredDict(name=b"user_passwords", env=interface.global_env, cache_on_set=True)
stored_user_services = interface.StoredDict(name=b"user_services", env=interface.global_env, cache_on_set=True)
@app.post("/user_signup")
@contract({"username": (0, str), "password": (0, str)})
async def user_signup(request):
    username = request.json["username"].encode()
    password = request.json["password"]
    if username in stored_user_passwords:
        return sanic_json({"status": "ERR", "message": "Username occupied"})
    stored_user_passwords[username] = hash_password(password).encode()
    stored_user_passwords.flush_buffer()
    stored_user_services[username] = b""
    stored_user_services.flush_buffer()
    # Signup
    return sanic_json({"status": "OK", "message": ""})

@app.route("/service/<service_name:str>")
async def service_with_name(_, service_name: str):
    return html(router.read("service_page"))

@app.post("/check_blob")
@contract({"service_name": (0, str), "blob_hash": (16, str)})
async def check_blob(request):
    name = request.json["service_name"]
    if not Services.service_exists(name):
        return sanic_json({"status": "ERR", "message": "Service doesn't exist"})
    bundle = get_service_obj(name).server_check(bytes.fromhex(request.json["blob_hash"]))
    return sanic_json({"status": "OK", "bundle": bundle, "message": ""})


@app.post("/has_service")
@contract({"service_name": (0, str)})
async def has_service(request):
    name = request.json["service_name"]
    if not Services.service_exists(name):
        return sanic_json({"status": "ERR", "message": "Service doesn't exist"})
    return sanic_json({"status": "OK", "message": ""})


@app.post("/get_service_metadata")
@contract({"service_name": (0, str)})
async def get_service_metadata(request):
    name = request.json["service_name"]
    if not Services.service_exists(name):
        return sanic_json({"status": "ERR", "message": "Service doesn't exist"})
    return sanic_json({"status": "OK", "metadata": Services.get_metadata(name), "message": ""})




@app.route("/")
async def index(_): return html(router.read("index"))

@app.route("/signup")
async def signup(_): return html(router.read("signup"))

@app.route("/dashboard")
async def dashboard(_): return html(router.read("dashboard"))

@app.route("/login")
async def login(_): return html(router.read("login"))

@app.route("/trees")
async def trees_page(_): return html(router.read("trees"))

@app.route("/service")
async def service(_):
    return html(router.read("service_page"))

@app.route("/services")
async def services(_): return html(router.read("services"))

@app.route("/hexdb")
async def hexdb_page(_): return html(router.read("hexdb_page"))

@app.route("/<filepath:path>")
async def serve_asset(_, filepath: str):
    path = Path(filepath)

    if len(path.parts) < 2 or not path.suffix:
        raise NotFound("Invalid asset path")

    ext = path.suffix.lstrip(".").lower()
    page = path.parts[0]
    asset = "/".join(path.parts[1:])
    #print(asset)

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
