import subprocess

def run(cmd):
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())
    return result.returncode

def drop_typesense():
    container_name = "typesense"

    # Stop container
    run(["docker", "stop", container_name])

    # Remove /data inside container (after restart)
    run([
        "docker", "run", "--rm",
        "--volumes-from", container_name,
        "busybox", "sh", "-c", "rm -rf /data/*"
    ])

    print("Typesense data dropped successfully.")

if __name__ == "__main__":
    drop_typesense()
