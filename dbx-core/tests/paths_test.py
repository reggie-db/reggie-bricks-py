import importlib
import sys

from dbx_core import paths


def test_module_path_resolves_from_file_path(tmp_path):
    module_file = tmp_path / "x.py"
    templates_dir = tmp_path / "templates"
    target = templates_dir / "xzy.txt"
    templates_dir.mkdir(parents=True, exist_ok=True)
    module_file.write_text("# test module\n")
    target.write_text("hello")

    resolved = paths.module_path(module_file, "templates", "xzy.txt")

    assert resolved == target.resolve()


def test_module_path_resolves_from_module_name(tmp_path):
    pkg_root = tmp_path / "demo_pkg"
    pkg_root.mkdir(parents=True, exist_ok=True)
    (pkg_root / "__init__.py").write_text("")
    (pkg_root / "x.py").write_text("VALUE = 1\n")
    templates_dir = pkg_root / "templates"
    templates_dir.mkdir(parents=True, exist_ok=True)
    target = templates_dir / "xzy.txt"
    target.write_text("hello")

    sys.path.insert(0, str(tmp_path))
    try:
        module_name = "demo_pkg.x"
        importlib.import_module(module_name)
        resolved = paths.module_path(module_name, "templates", "xzy.txt")
    finally:
        sys.path.pop(0)
        sys.modules.pop("demo_pkg.x", None)
        sys.modules.pop("demo_pkg", None)

    assert resolved == target.resolve()


def test_module_path_resolves_from_module_object(tmp_path):
    pkg_root = tmp_path / "obj_pkg"
    pkg_root.mkdir(parents=True, exist_ok=True)
    (pkg_root / "__init__.py").write_text("")
    (pkg_root / "x.py").write_text("VALUE = 1\n")
    templates_dir = pkg_root / "templates"
    templates_dir.mkdir(parents=True, exist_ok=True)
    target = templates_dir / "xzy.txt"
    target.write_text("hello")

    sys.path.insert(0, str(tmp_path))
    try:
        module = importlib.import_module("obj_pkg.x")
        resolved = paths.module_path(module, "templates", "xzy.txt")
        resolved_from_name = paths.module_path(module.__name__, "templates", "xzy.txt")
    finally:
        sys.path.pop(0)
        sys.modules.pop("obj_pkg.x", None)
        sys.modules.pop("obj_pkg", None)

    assert resolved == target.resolve()
    assert resolved_from_name == target.resolve()


def test_module_path_returns_none_for_missing_resource(tmp_path):
    module_file = tmp_path / "x.py"
    module_file.write_text("# test module\n")

    assert paths.module_path(module_file, "templates", "missing.txt") is None
