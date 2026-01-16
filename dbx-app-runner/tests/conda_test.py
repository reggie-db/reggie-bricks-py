import asyncio
import logging

from dbx_app_runner import conda, docker


async def main():
    print(conda.dependency_name("caddy"))
    print(conda.dependency_name("caddy::caddy"))
    print(conda.dependency_name("cool::caddy=12"))
    deps = [
        "caddy",
        "curl",
    ]
    if not docker.path():
        deps.append("udocker")
    conda.update("cool", *deps)
    conda.run("cool")("caddy", "--version", _bg=True).wait()
    conda.run("cool")(
        docker.path() or "udocker",
        "run",
        "hello-world",
        _bg=True,
    ).wait()
    conda.update(
        "jarvis", "caddy", "conda-forge::openjdk=17", pip_dependencies=["requests"]
    )
    conda.run("jarvis")("java", "--version", _bg=True).wait()
    conda.run("jarvis")("which", "java", _bg=True).wait()


if __name__ == "__main__":
    logging.getLogger().info("test")
    asyncio.run(main())
