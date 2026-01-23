import functools
import os
import uuid

from dbx_tools import clients
from lfp_logging import logs
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, declarative_base

LOG = logs.logger()

Base = declarative_base()


def get_db():
    with Session(get_engine()) as db:
        yield db


@functools.cache
def get_engine():
    instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME", "reggie-pierce-cv-dev")
    postgres_database = os.environ.get("LAKEBASE_DATABASE_NAME", "databricks_postgres")

    wc = clients.workspace_client()
    db_instance = wc.database.get_database_instance(instance_name)
    postgres_username = wc.current_user.me().user_name

    postgres_host = db_instance.read_write_dns
    postgres_port = 5432
    db_connection_string = f"postgresql+psycopg2://{postgres_username}:@{postgres_host}:{postgres_port}/{postgres_database}"
    LOG.info(f"Using connection string: {db_connection_string}")
    print(f"Using connection string: {db_connection_string}")
    engine = create_engine(db_connection_string)

    @event.listens_for(engine, "do_connect")
    def provide_token(dialect, conn_rec, cargs, cparams):
        """Provide the App's OAuth token. Caching is managed by WorkspaceClient"""
        cparams["password"] = wc.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[instance_name]
        ).token

    return engine
