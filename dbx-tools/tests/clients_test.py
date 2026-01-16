"""
Quick tests for `dbx_tools.clients` moved out of the source module.
"""

import os

from dbx_tools.clients import spark, workspace_client

if __name__ == "__main__":
    os.environ["DATABRICKS_CONFIG_PROFILE"] = "FIELD-ENG-EAST"
    print(spark())
    print(spark())
    print(workspace_client().catalogs.list(max_results=10))
