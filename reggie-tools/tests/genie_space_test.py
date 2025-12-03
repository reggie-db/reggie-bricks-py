import os

from reggie_tools import clients

if __name__ == "__main__":
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "FIELD-ENG-EAST")
    wc = clients.workspace_client()
    resp: dict = wc.genie._api.do(
        "GET",
        "/api/2.0/genie/spaces/01f0cfa53c571bbb9b36f0e14a4e408d?include_serialized_space=true",
    )
    print(resp)
    print(type(resp))
    print(resp.get("serialized_space"))
    print(type(resp.get("serialized_space")))
