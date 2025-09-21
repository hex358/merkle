#!/usr/bin/env python3
import argparse
import requests
import json

BASE_URL = "http://127.0.0.1:8000"

def post(endpoint: str, data: dict):
    url = f"{BASE_URL}{endpoint}"
    r = requests.post(url, json=data)
    try:
        return r.json()
    except Exception:
        return {"status": "ERR", "message": f"Non-JSON response: {r.text}"}

def main():
    parser = argparse.ArgumentParser(description="CertumTree CLI")
    sub = parser.add_subparsers(dest="cmd")

    # user signup
    s = sub.add_parser("signup")
    s.add_argument("username")
    s.add_argument("password")

    # user login
    l = sub.add_parser("login")
    l.add_argument("username")
    l.add_argument("password")

    # register service
    r = sub.add_parser("register")
    r.add_argument("username")
    r.add_argument("password")
    r.add_argument("service_name")

    # delete service
    d = sub.add_parser("delete")
    d.add_argument("username")
    d.add_argument("password")
    d.add_argument("service_name")

    # list services of a user
    ls = sub.add_parser("myservices")
    ls.add_argument("username")

    # get service token
    gt = sub.add_parser("gettoken")
    gt.add_argument("username")
    gt.add_argument("password")
    gt.add_argument("service_name")

    # add blob
    ab = sub.add_parser("addblob")
    ab.add_argument("token")
    ab.add_argument("service_name")
    ab.add_argument("blob_hash", help="32-char hex string")

    # check blob
    cb = sub.add_parser("checkblob")
    cb.add_argument("service_name")
    cb.add_argument("blob_hash", help="32-char hex string")

    args = parser.parse_args()

    if args.cmd == "signup":
        print(post("/user_signup", {"username": args.username, "password": args.password}))

    elif args.cmd == "login":
        print(post("/user_login", {"username": args.username, "password": args.password}))

    elif args.cmd == "register":
        for i in range(500):
            print(post("/register_service", {
                "username": args.username,
                "password": args.password,
                "service_name": args.service_name + str(i) + "fff"
            }))

    elif args.cmd == "delete":
        print(post("/delete_service", {
            "username": args.username,
            "password": args.password,
            "service_name": args.service_name
        }))

    elif args.cmd == "myservices":
        print(post("/get_my_services", {"username": args.username}))

    elif args.cmd == "gettoken":
        print(post("/get_token", {
            "username": args.username,
            "password": args.password,
            "service_name": args.service_name
        }))

    elif args.cmd == "addblob":
        print(kief(args.blob_hash.encode())) # 830515e6ba304f76
        print(post("/add_blob", {
            "token": args.token,
            "service_name": args.service_name,
            "blob_hash": kief(args.blob_hash.encode())
        }))

    elif args.cmd == "checkblob":
        print(post("/check_blob", {
            "service_name": args.service_name,
            "blob_hash": kief(args.blob_hash.encode())
        }))

    else:
        parser.print_help()

import hashlib
def kief(*args) -> bytes:
	h = hashlib.blake2b(digest_size=8)
	for a in args:
		if isinstance(a, memoryview):
			a = bytes(a)
		# assume other args are bytes
		h.update(a)
	return h.digest().hex()

if __name__ == "__main__":
    main()
