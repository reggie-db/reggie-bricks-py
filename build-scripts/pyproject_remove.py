import utils

root = utils.repo_root()
root_venv = root / ".venv"
build_scripts_root = utils.build_scripts_root()


def run():
    for pyproject in utils.workspace_pyprojects():
        with pyproject.content() as content:
            _modify_project(content)


def _modify_project(content: dict):
    _delete_key(content, "build-system")
    _delete_key(content, "tool.uv.sources")
    project = content.get("project", None)
    if project:
        _delete_key(project, "optional-dependencies")
        _modify_dependencies(project["dependencies"])


def _modify_dependencies(dependencies: list):
    if not dependencies:
        return
    for i in range(len(dependencies)):
        dependency = dependencies[i]
        before, sep, _ = dependency.partition("file://${PROJECT_ROOT}")
        if sep:
            before, sep, _ = before.partition(" @ ")
            if sep:
                before = before.strip()
            if before:
                dependencies[i] = before


def _delete_key(content: dict, *keys: str):
    keys = [part for k in keys for part in k.split(".")]

    def _delete(data: dict, idx: int):
        key = keys[idx]
        last = idx == len(keys) - 1

        if last and key in data:
            del data[key]
        else:
            value = data.get(key, None)
            if isinstance(value, dict):
                _delete(value, idx + 1)
                if len(value) == 0:
                    del data[key]

    _delete(content, 0)


if __name__ == "__main__":
    run()
