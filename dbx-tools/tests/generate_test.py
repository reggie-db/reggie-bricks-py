from databricks.sdk import WorkspaceClient

from dbx_tools import configs

if "__main__" == __name__:
    cfg = configs.get()
    wc = WorkspaceClient(config=cfg)
    wc.authorize()
