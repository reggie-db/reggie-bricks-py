import subprocess

from reggie_app_runner import docker


def _podman_socket():
    out = subprocess.check_output(
        ["/opt/podman/bin/podman", "info", "--format", "{{.Host.RemoteSocket.Path}}"],
        text=True,
    ).strip()
    if not out:
        raise RuntimeError("no podman remote socket")
    return out


if __name__ == "__main__":
    print(_podman_socket())
    print(docker._conda_env_name())
    if True:
        print(docker.image_hash("plexinc/pms-docker"))
        print(docker.image_hash("plexinc/pms-docker:1.42.2.10156-f737b826c"))
        docker.pull("plexinc/pms-docker")
    print(docker.command()("version", _bg=True).wait())
    print(docker.command()("run", "hello-world", _bg=True).wait())
    print(docker.command()("run", "ealen/echo-server", _bg=True).wait())
