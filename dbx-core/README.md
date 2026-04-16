# dbx-core

Shared foundational utilities for workspace packages.

## What It Includes

- `objects.py`: serialization (`dump`, `to_json`), hashing, attribute traversal, and key removal.
- `paths.py`: path resolution helpers with temp/home fallbacks.
- `parsers.py`: small coercion helpers (`to_bool`, `to_float`, and related parsing utilities).
- `projects.py`: root/workspace project discovery and version helpers.
- `strs.py`: tokenization and string normalization helpers.
- `imports.py`: safe module and attribute resolution helpers.
- `locks.py`: lock protocol helpers used by higher-level packages.

## Usage

```python
from dbx_core import objects, paths
from lfp_logging import logs

log = logs.logger()
data = objects.dump({"ok": True})
cfg = paths.path("~/config", exists=False)
```

## Dependencies

- `lfp-logging`

