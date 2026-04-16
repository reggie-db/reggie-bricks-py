# dbx-postgres

Postgres helpers for local development and SQLAlchemy engine bootstrapping.

## What It Provides

- `postgres.py`
  - `create_engine(...)` and `create_async_engine(...)` wrappers that ensure the target database exists.
  - Optional table creation and bootstrap SQL execution.
- `local_postgres.py`
  - `LocalPostgres` helper for local unix-socket Postgres lifecycle (`up`, `down`, `url`).

## Usage

```python
from dbx_postgres.local_postgres import LocalPostgres
from dbx_postgres import postgres

svc = LocalPostgres()
engine = postgres.create_engine(svc.url("example_db"))
```
