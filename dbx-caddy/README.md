# dbx-caddy

Install and run the Caddy binary for local development workflows.

## What It Provides

- `path()`: returns a usable `caddy` binary path, installing it when needed.
- `install()`: downloads and installs the correct binary for the current OS/arch.
- `run()`: context manager for running `caddy run --config ...` with file-path or inline config input.

## Usage

```python
from dbx_caddy.caddy import run

with run(":8080 { respond \"ok\" 200 }") as proc:
    ...
```
