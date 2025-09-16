import docker

client = docker.from_env()

try:
    # Try to get existing container
    container = client.containers.get("typesense")
    if container.status != "running":
        container.start()
    #    print("Started existing container:", container.id)
    #else:
    #    print("Container is already running:", container.id)
except docker.errors.NotFound:
    # If it doesn’t exist, create and run it
    container = client.containers.run(
        "typesense/typesense:0.25.1",
        detach=True,
        name="typesense",
        ports={"8108/tcp": 8108},
        volumes={"/tmp/typesense-data": {"bind": "/data", "mode": "rw"}},
        command=["--data-dir", "/data", "--api-key=xyz", "--enable-cors"]
    )
    #print("Created and started new container:", container.id)