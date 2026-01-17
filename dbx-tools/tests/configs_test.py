"""
Quick tests for `dbx_tools.configs` moved out of the source module.

Exercises config discovery helpers to avoid running prints during library import.
"""

from typing import Iterable

from dbx_tools import clients, configs
from dbx_tools.configs import _get_all, value

if __name__ == "__main__":
    print(configs.token())
    test = None
    print(getattr(test, "conf", None))
    print(isinstance({}, Iterable))
    print(_get_all(clients.spark(), "conf"))
    print(value("spark.databricks.execution.timeout"))
    print(value("spark.databricks.execution.timeout2"))
    print(value("HOME"))
    clients.spark().sql("select 'hello there' as msg").show()
