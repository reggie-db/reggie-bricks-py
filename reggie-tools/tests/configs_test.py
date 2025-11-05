"""
Quick tests for `reggie_tools.configs` moved out of the source module.

Exercises config discovery helpers to avoid running prints during library import.
"""

from typing import Iterable

from reggie_tools import clients
from reggie_tools.configs import _get_all, config_value, get


if __name__ == "__main__":
    test = None
    print(getattr(test, "conf", None))
    print(isinstance({}, Iterable))
    print(_get_all(clients.spark(), "conf"))
    print(config_value("spark.databricks.execution.timeout"))
    print(config_value("spark.databricks.execution.timeout2"))
    print(config_value("HOME"))
    clients.spark().sql("select 'hello there' as msg").show()


