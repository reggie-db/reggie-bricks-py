import os

from reggie_tools import clients, genie

if __name__ == "__main__":
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "FIELD-ENG-EAST")
    genie_space_id = "01f0cfa53c571bbb9b36f0e14a4e408d"
    wc = clients.workspace_client()
    genie.Service(wc, genie_space_id)
