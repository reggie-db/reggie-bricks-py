#!/usr/bin/env bash
set -euo pipefail

exec uv run --with git+https://github.com/reggie-db/reggie-build-py.git reggie-build "$@"