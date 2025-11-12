"""
OpenAPI code generation utilities.

Generates FastAPI code from OpenAPI specs and syncs generated files with change detection.
"""

import hashlib
import re
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
import utils
from scripts import projects
import warnings

warnings.filterwarnings(
    "ignore",
    message="Valid config keys have changed in V2",
    category=UserWarning,
    module="pydantic",
)
import fastapi_code_generator.__main__ as fastapi_code_generator_main  # noqa: E402


LOG = utils.logger()
_TIMESTAMP_RE = re.compile(rb"^\s*#\s*timestamp:.*$")


def sync_generated_code(input_dir: Path, output_dir: Path) -> None:
    """Sync generated code from input_dir to output_dir if any relative file differs."""
    if not input_dir.exists():
        raise FileNotFoundError(f"Missing input directory: {input_dir}")

    input_dir, output_dir = input_dir.resolve(), output_dir.resolve()
    input_files, output_files = _list_files(input_dir), _list_files(output_dir)

    input_hash, output_hash = (
        _hash_files(input_dir, input_files),
        _hash_files(output_dir, output_files),
    )

    changed = [
        f
        for f in sorted(set(input_hash) | set(output_hash))
        if input_hash.get(f) != output_hash.get(f)
    ]

    if not changed:
        LOG.info("No changes detected")
        return

    if extra := (output_files - input_files):
        raise ValueError(f"Extra files in output: {sorted(extra)}")

    LOG.info("Changed files:\n" + "\n".join(f"  {f}" for f in changed))

    if output_dir.exists():
        shutil.rmtree(output_dir)
    shutil.copytree(input_dir, output_dir)
    LOG.info(f"Synchronized {output_dir}")


def _list_files(directory: Path) -> set[str]:
    """Return relative file paths from directory, skipping caches and compiled files."""
    if not directory.is_dir():
        return set()
    return {
        str(p.relative_to(directory))
        for p in directory.rglob("*")
        if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc"
    }


def _hash_files(dir: Path, rel_files: set[str]) -> dict[str, str]:
    """Return SHA-256 hashes for given relative file paths, ignoring timestamp comments."""
    out = {}
    for file in sorted(rel_files):
        h = hashlib.sha256()
        h.update(file.encode())
        with open(dir / file, "rb") as f:
            for line in f:
                if not _TIMESTAMP_RE.match(line):
                    decoded_line = line.decode("utf-8", errors="ignore")
                    decoded_line = decoded_line.replace("\\'", '\\"').replace("'", '"')
                    h.update(decoded_line.encode())
        out[file] = h.hexdigest()
    return out


if __name__ == "__main__":
    src = Path("~/Desktop/open-api.yaml").expanduser()
    tmpl = Path(__file__).parent / "openapi_template"
    out = projects.root_dir() / "demo-iot/src/demo_iot_generated"

    for _ in utils.watch_file(src):
        with TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            LOG.info(f"Generating code: {src} → {out}")
            try:
                fastapi_code_generator_main.app(
                    [
                        "--input",
                        str(src),
                        "--output",
                        str(tmp_dir),
                        "--template-dir",
                        str(tmpl),
                    ]
                )
            except SystemExit as e:
                if e.code:
                    raise
            (tmp_dir / "__init__.py").touch()
            sync_generated_code(tmp_dir, out)
