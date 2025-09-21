def main():
    import docker
    client = docker.from_env()

    try:
        container = client.containers.get("typesense")
        if container.status != "running":
            container.start()
    except docker.errors.NotFound:
        container = client.containers.run(
            "typesense/typesense:0.25.1",
            detach=True,
            name="typesense",
            ports={"8108/tcp": 8108},
            volumes={"/tmp/typesense-data": {"bind": "/data", "mode": "rw"}},
            command=["--data-dir", "/data", "--api-key=xyz", "--enable-cors"]
        )