import functools
import os
import time
import uuid

from dbx_tools import clients
from lfp_logging import logs
from sqlalchemy import create_engine, event
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session, declarative_base

LOG = logs.logger()

Base = declarative_base()


def _lakebase_settings() -> tuple[str, str, bool, float]:
    """
    Read Lakebase and SQL logging settings from environment.
    """

    instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME", "reggie-pierce-cv-dev")
    postgres_database = os.environ.get("LAKEBASE_DATABASE_NAME", "databricks_postgres")
    log_sql = os.environ.get("DBX_SQL_LOG", "0") == "1"
    slow_sql_ms = float(os.environ.get("DBX_SQL_SLOW_MS", "250"))
    return instance_name, postgres_database, log_sql, slow_sql_ms


def _pool_settings() -> tuple[int, int, float]:
    """
    Read SQLAlchemy pool sizing from environment.
    """

    pool_size = int(os.environ.get("DBX_SQL_POOL_SIZE", "10"))
    max_overflow = int(os.environ.get("DBX_SQL_MAX_OVERFLOW", "20"))
    pool_timeout = float(os.environ.get("DBX_SQL_POOL_TIMEOUT", "30"))
    return pool_size, max_overflow, pool_timeout


def _install_token_injection(*, engine, wc, instance_name: str) -> None:
    """
    Inject the Databricks Lakebase OAuth token during connection.
    """

    @event.listens_for(engine, "do_connect")
    def provide_token(dialect, conn_rec, cargs, cparams):
        """Provide the App's OAuth token. Caching is managed by WorkspaceClient"""
        cparams["password"] = wc.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[instance_name]
        ).token


def _install_sql_logging(*, engine, log_sql: bool, slow_sql_ms: float) -> None:
    """
    Install SQL logging + simple slow query logging on an engine.

    Parameter interpolation is driver-specific; we log statement and params
    separately to avoid misleading or unsafe "string substitution".
    """

    @event.listens_for(engine, "before_cursor_execute")
    def _before_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ):
        context._query_start_perf_counter = time.perf_counter()
        if log_sql:
            LOG.info("SQL: %s", statement)
            LOG.info("SQL params: %s", parameters)

    @event.listens_for(engine, "after_cursor_execute")
    def _after_cursor_execute(
        conn, cursor, statement, parameters, context, executemany
    ):
        start = getattr(context, "_query_start_perf_counter", None)
        if start is None:
            return
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        if elapsed_ms >= slow_sql_ms:
            LOG.info("Slow SQL %.1fms: %s", elapsed_ms, statement)


def _lakebase_url(
    *, drivername: str, wc, instance_name: str, postgres_database: str
) -> tuple[URL, str]:
    """
    Build a SQLAlchemy URL for Lakebase using the given driver name.

    Uses `URL.create(...)` so usernames with `@` are correctly encoded.
    """

    db_instance = wc.database.get_database_instance(instance_name)
    postgres_username = wc.current_user.me().user_name

    postgres_host = db_instance.read_write_dns
    postgres_port = 5432
    url = URL.create(
        drivername,
        username=postgres_username,
        password=None,
        host=postgres_host,
        port=postgres_port,
        database=postgres_database,
    )
    return url, url.render_as_string(hide_password=True)


def get_db():
    with Session(get_engine()) as db:
        yield db


async def get_async_db():
    """
    Yield an AsyncSession for async endpoints.

    This uses an async driver (asyncpg) and does not block the event loop.
    """

    async with AsyncSession(get_async_engine()) as db:
        yield db


@functools.cache
def get_engine():
    instance_name, postgres_database, log_sql, slow_sql_ms = _lakebase_settings()
    wc = clients.workspace_client()
    url, rendered_url = _lakebase_url(
        drivername="postgresql+psycopg2",
        wc=wc,
        instance_name=instance_name,
        postgres_database=postgres_database,
    )
    LOG.info(f"Using connection string: {rendered_url}")
    # Enable SQLAlchemy engine echo with `DBX_SQL_LOG=1`.
    pool_size, max_overflow, pool_timeout = _pool_settings()
    engine = create_engine(
        url,
        echo=log_sql,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
    )

    _install_token_injection(engine=engine, wc=wc, instance_name=instance_name)
    _install_sql_logging(engine=engine, log_sql=log_sql, slow_sql_ms=slow_sql_ms)

    return engine


@functools.cache
def get_async_engine():
    """
    Return an AsyncEngine backed by asyncpg.

    Requirements:
    - `asyncpg` installed
    - `greenlet` installed (required by SQLAlchemy asyncio)

    The Databricks Lakebase OAuth token is injected at connect time.
    """

    instance_name, postgres_database, log_sql, slow_sql_ms = _lakebase_settings()
    wc = clients.workspace_client()
    url, rendered_url = _lakebase_url(
        drivername="postgresql+asyncpg",
        wc=wc,
        instance_name=instance_name,
        postgres_database=postgres_database,
    )
    LOG.info(f"Using async connection string: {rendered_url}")

    pool_size, max_overflow, pool_timeout = _pool_settings()
    async_engine = create_async_engine(
        url,
        echo=log_sql,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
    )

    # Events must be registered on the underlying sync engine.
    sync_engine = async_engine.sync_engine

    _install_token_injection(engine=sync_engine, wc=wc, instance_name=instance_name)
    _install_sql_logging(engine=sync_engine, log_sql=log_sql, slow_sql_ms=slow_sql_ms)

    return async_engine
