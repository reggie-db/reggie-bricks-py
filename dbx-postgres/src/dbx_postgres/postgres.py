import logging
import time

from lfp_logging import logs
from sqlalchemy import URL, Engine, text
from sqlalchemy import create_engine as sql_create_engine
from sqlalchemy.ext.asyncio import create_async_engine as sql_create_async_engine
from sqlalchemy.orm import DeclarativeBase

LOG = logs.logger()

_DEFAULT_ADMIN_DATABASE = "postgres"
_DEFAULT_DATABASE_TIMEOUT = 15.0


def create_engine(
    url: URL,
    *tables: type[DeclarativeBase],
    statements: list[str] | None = None,
    admin_database: str = _DEFAULT_ADMIN_DATABASE,
    database_timeout: float = _DEFAULT_DATABASE_TIMEOUT,
    **kwargs,
):
    return _create_engine(
        url,
        *tables,
        statements=statements,
        admin_database=admin_database,
        database_timeout=database_timeout,
        **kwargs,
    )


def create_async_engine(
    url: URL,
    *tables: type[DeclarativeBase],
    statements: list[str] | None = None,
    admin_database: str = _DEFAULT_ADMIN_DATABASE,
    database_timeout: float = _DEFAULT_DATABASE_TIMEOUT,
    **kwargs,
):
    _create_engine(
        url,
        *tables,
        statements=statements,
        admin_database=admin_database,
        database_timeout=database_timeout,
    ).dispose()
    return sql_create_async_engine(url, **kwargs)


def _create_engine(
    url: URL,
    *tables: type[DeclarativeBase],
    statements: list[str] | None = None,
    admin_database: str,
    database_timeout: float,
    **kwargs,
):
    def _create(timeout: float | None = None) -> Engine | None:
        timeout_at = time.time() + timeout if timeout else None
        while True:
            try:
                engine = sql_create_engine(url, **kwargs)
                try:
                    with engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                        return engine
                except Exception:
                    engine.dispose()
                    raise
            except Exception:
                log_msg = "Database not online - url:%s"
                log_args = (url,)
                log_level = logging.DEBUG
                if timeout_at is None:
                    sleep = None
                else:
                    sleep = min(timeout_at - time.time(), 0.15)
                    if not sleep:
                        log_level = logging.WARNING
                LOG.log(log_level, log_msg, *log_args, exc_info=True)
                if sleep:
                    time.sleep(sleep)
                    continue
                return None

    db_engine = _create()
    if db_engine is None:
        admin_engine = sql_create_engine(
            url.set(database=admin_database),
            isolation_level="AUTOCOMMIT",
        )
        try:
            with admin_engine.connect() as conn:
                exists = conn.exec_driver_sql(
                    "SELECT 1 FROM pg_database WHERE datname = %s", (url.database,)
                ).scalar()
                if not exists:
                    conn.exec_driver_sql(f'CREATE DATABASE "{url.database}"')
        finally:
            admin_engine.dispose()
        db_engine = _create(database_timeout)
        if not db_engine:
            raise TimeoutError(
                f"Database not online - url:{url} database_timeout:{database_timeout}"
            )
    if tables or statements:
        # noinspection PyTypeChecker
        with db_engine.begin() as conn:
            # create tables
            if tables:
                for table in tables:
                    table.metadata.create_all(conn)

            # install triggers / functions
            if statements:
                for statement in statements:
                    conn.execute(text(statement))
    return db_engine
