"""
Quick tests for `dbx_core.projects` moved from the source module.

This mirrors the original ad hoc prints from `projects.py` under a test location
to avoid execution in the library module itself.
"""

from pathlib import Path

from dbx_core.projects import _remote_origin_name, name

if __name__ == "__main__":
    print(name())
    print(name())
    print(name(Path.cwd()))
    print(name(__file__))
    print(_remote_origin_name())
