import argparse
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path

# Migration utility for adopting databricks-tools-core.
# The script runs in dry-run mode by default and only writes files when --apply is set.

LOG = logging.getLogger("migrate_to_databricks_tools_core")
_PYPROJECT = "pyproject.toml"
_CORE_SOURCE_BLOCK = (
    "[tool.uv.sources.databricks-tools-core]\n"
    'git = "https://github.com/databricks-solutions/ai-dev-kit.git"\n'
    'subdirectory = "databricks-tools-core"\n'
)
_WORKSPACE_DEPS = (
    '"dbx-tools @ file://${PROJECT_ROOT}/../dbx-tools"',
    '"dbx-tools"',
)


@dataclass
class FileChange:
    path: Path
    changed: bool
    notes: list[str]


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write(path: Path, content: str, apply: bool) -> None:
    if apply:
        path.write_text(content, encoding="utf-8")


def _ensure_core_dependency(content: str) -> tuple[str, list[str]]:
    notes: list[str] = []
    updated = content

    if '"databricks-tools-core"' not in updated:
        dep_block_match = re.search(
            r"dependencies\s*=\s*\[(?P<body>.*?)\]", updated, re.S
        )
        if dep_block_match:
            body = dep_block_match.group("body")
            replacement = body
            if body.strip():
                replacement = f'{body}\n    "databricks-tools-core",'
            else:
                replacement = '\n    "databricks-tools-core",\n'
            updated = (
                updated[: dep_block_match.start("body")]
                + replacement
                + updated[dep_block_match.end("body") :]
            )
            notes.append("added databricks-tools-core dependency")
        else:
            notes.append("manual review: dependencies array not found")

    for dep in _WORKSPACE_DEPS:
        if dep in updated:
            updated = updated.replace(dep, '"databricks-tools-core"')
            notes.append("replaced dbx-tools dependency with databricks-tools-core")

    if "[tool.uv.sources.databricks-tools-core]" not in updated:
        updated = updated.rstrip() + "\n\n" + _CORE_SOURCE_BLOCK + "\n"
        notes.append("added tool.uv source for databricks-tools-core")

    return updated, notes


def _remove_clients_from_import(line: str) -> tuple[str | None, bool]:
    match = re.match(r"^from\s+dbx_tools\s+import\s+(.+)$", line.strip())
    if not match:
        return line, False

    imports = [part.strip() for part in match.group(1).split(",") if part.strip()]
    if "clients" not in imports:
        return line, False
    filtered = [part for part in imports if part != "clients"]
    if not filtered:
        return None, True
    return f"from dbx_tools import {', '.join(filtered)}", True


def _add_core_import(lines: list[str]) -> list[str]:
    core_import = "from databricks_tools_core import get_workspace_client"
    if any(line.strip() == core_import for line in lines):
        return lines

    insert_idx = 0
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("import ") or stripped.startswith("from "):
            insert_idx = idx + 1
    return [*lines[:insert_idx], core_import, *lines[insert_idx:]]


def _migrate_python_file(content: str) -> tuple[str, list[str]]:
    notes: list[str] = []
    updated = content

    if "clients.workspace_client(" not in updated:
        return updated, notes

    updated = updated.replace("clients.workspace_client(", "get_workspace_client(")
    notes.append("replaced clients.workspace_client() calls")

    original_lines = updated.splitlines()
    changed_import = False
    new_lines: list[str] = []
    for line in original_lines:
        replaced, changed = _remove_clients_from_import(line)
        changed_import = changed_import or changed
        if replaced is not None:
            new_lines.append(replaced)

    if changed_import:
        notes.append("updated dbx_tools import list to remove clients")
    updated_lines = _add_core_import(new_lines)
    if updated_lines != new_lines:
        notes.append("added databricks_tools_core import")

    return "\n".join(updated_lines) + ("\n" if content.endswith("\n") else ""), notes


def _process_pyproject(path: Path, apply: bool) -> FileChange:
    original = _read(path)
    updated, notes = _ensure_core_dependency(original)
    changed = updated != original
    _write(path, updated, apply)
    return FileChange(path=path, changed=changed, notes=notes)


def _process_python(path: Path, apply: bool) -> FileChange:
    original = _read(path)
    updated, notes = _migrate_python_file(original)
    changed = updated != original
    _write(path, updated, apply)
    return FileChange(path=path, changed=changed, notes=notes)


def _collect_targets(root: Path) -> tuple[list[Path], list[Path]]:
    pyprojects = sorted(root.rglob(_PYPROJECT))
    python_files = sorted(
        p
        for p in root.rglob("*.py")
        if ".venv" not in p.parts and "site-packages" not in p.parts
    )
    return pyprojects, python_files


def _to_report(
    pyproject_changes: list[FileChange], python_changes: list[FileChange]
) -> dict[str, object]:
    changed_files = [
        {"path": str(change.path), "notes": change.notes}
        for change in [*pyproject_changes, *python_changes]
        if change.changed
    ]
    manual_review = [
        {"path": str(change.path), "notes": change.notes}
        for change in [*pyproject_changes, *python_changes]
        if any(note.startswith("manual review:") for note in change.notes)
    ]
    return {
        "changed_files": changed_files,
        "manual_review": manual_review,
        "summary": {
            "changed_count": len(changed_files),
            "manual_review_count": len(manual_review),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate dbx-tools usage to databricks-tools-core."
    )
    parser.add_argument(
        "--root",
        default=".",
        help="Repository root to migrate (defaults to current directory).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes to disk. Without this flag the script is dry-run only.",
    )
    parser.add_argument(
        "--report",
        default="migration-report.json",
        help="Path for JSON report relative to --root.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logs.",
    )
    args = parser.parse_args()
    _configure_logging(args.verbose)

    root = Path(args.root).resolve()
    pyprojects, python_files = _collect_targets(root)
    LOG.info(
        "Scanning %s pyproject files and %s python files",
        len(pyprojects),
        len(python_files),
    )

    pyproject_changes = [_process_pyproject(path, args.apply) for path in pyprojects]
    python_changes = [_process_python(path, args.apply) for path in python_files]

    report = _to_report(pyproject_changes, python_changes)
    report_path = (root / args.report).resolve()
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    LOG.info("Migration mode: %s", "apply" if args.apply else "dry-run")
    LOG.info("Changed files: %s", report["summary"]["changed_count"])
    LOG.info("Manual review items: %s", report["summary"]["manual_review_count"])
    LOG.info("Report written to %s", report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
