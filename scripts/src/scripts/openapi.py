"""
OpenAPI code generation utilities.

Generates FastAPI code from OpenAPI specs and syncs generated files with change detection.
"""

import difflib
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
_QUOTES_RE = re.compile(r"['\"]")
_TIMESTAMP_RE = re.compile(rb"^\s*#\s*timestamp:.*$")


def sync_generated_code(input_dir: Path, output_dir: Path) -> None:
    """Sync generated code from input_dir to output_dir if any relative file differs."""
    if not input_dir.exists():
        raise FileNotFoundError(f"Missing input directory: {input_dir}")

    input_dir, output_dir = input_dir.resolve(), output_dir.resolve()
    input_files, output_files = _list_files(input_dir), _list_files(output_dir)

    changed = []
    for file in set(input_files + output_files):
        if _files_diff(input_dir / file, output_dir / file):
            changed.append(file)

    if not changed:
        LOG.info("No changes detected")
        return

    LOG.info("Changed files:\n" + "\n".join(f"  {f}" for f in changed))

    if output_dir.exists():
        shutil.rmtree(output_dir)
    shutil.copytree(input_dir, output_dir)
    LOG.info(f"Synchronized {output_dir}")


def _list_files(directory: Path) -> list[str]:
    """Return relative file paths from directory, skipping caches and compiled files."""
    if not directory.is_dir():
        return []
    files = {
        str(p.relative_to(directory))
        for p in directory.rglob("*")
        if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc"
    }
    return sorted(list(files))


def _files_diff(file1: Path, file2: Path) -> bool:
    """Return True if files differ (ignoring quote differences), False if equivalent."""

    def _read_lines(file: Path):
        lines = []
        if file.exists():
            with open(file, "rb") as f:
                for line in f:
                    if not _TIMESTAMP_RE.match(line):
                        lines.append(line.decode("utf-8", errors="ignore"))
        return lines

    def _normalize_quotes(line: str) -> str:
        return _QUOTES_RE.sub(lambda m: '"' if m.group(0) == "'" else "'", line)

    lines1 = _read_lines(file1)
    lines2 = _read_lines(file2)

    # Generate diff on original lines
    diff = difflib.unified_diff(lines1, lines2, n=0)
    for line in diff:
        # Consider only changed lines (ignore headers like --- +++)
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---")):
            # Extract normalized comparison lines for equality ignoring quotes
            normalized = _normalize_quotes(line[1:])
            counterpart_list = lines2 if line.startswith("-") else lines1
            if all(_normalize_quotes(c) != normalized for c in counterpart_list):
                return True
    return False


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
