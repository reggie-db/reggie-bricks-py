"""Manual check entrypoint for verifying Databricks config discovery (moved)."""

from reggie_tools import configs


if __name__ == "__main__":
    print(configs.get())


