"""
Quick tests for `dbx_tools.clients` moved out of the source module.
"""

from dbx_tools import configs, runtimes
from dbx_tools.clients import spark, workspace_client

if __name__ == "__main__":
    # os.environ["DATABRICKS_CONFIG_PROFILE"] = "E2-DOGFOOD"
    print(configs.get() is not None)
    print(spark())
    print(spark())
    print(runtimes.context(None))
    print(list(workspace_client().schemas.list("reggie_pierce")))
