"""
Quick tests for `dbx_tools.funcs` moved out of the source module.

Mirrors the original ad hoc print-based checks for `infer_json` behavior using
Spark, without executing on import of the library package.
"""

import os

from dbx_tools import clients
from dbx_tools.funcs import infer_json

if __name__ == "__main__":
    os.environ["DATABRICKS_CONFIG_PROFILE"] = "FIELD-ENG-EAST"

    df = clients.spark().createDataFrame(
        [
            ('{"a":1,"b":2}',),
            ('[{"x":1},{"y":2}]',),
            ('"hello"',),
            ("42",),
            ("true",),
            ("null",),
            ("???",),
            (None,),
        ],
        ["json_col"],
    )

    df.withColumn(
        "wrapped_schema_only",
        infer_json("json_col", include_value=False, include_type=False),
    ).show(truncate=False)

    df.withColumn(
        "wrapped_value_only",
        infer_json("json_col", include_schema=False, include_type=False),
    ).show(truncate=False)

    df.withColumn("wrapped_with_type", infer_json("json_col", include_type=True)).show(
        truncate=False
    )
