import os

from reggie_tools import clients, genie
from reggie_tools.genie import GenieResponse


if __name__ == "__main__":
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "FIELD-ENG-EAST")
    genie_space_id = "01f0cfa53c571bbb9b36f0e14a4e408d"
    wc = clients.workspace_client()
    resp: dict = wc.genie._api.do(
        "GET",
        f"/api/2.0/genie/spaces/{genie_space_id}/conversations/{conversation_id}/messages/{message_id}",
    )
    print(resp)
    print(type(resp))
    print(resp.get("serialized_space"))
    print(type(resp.get("serialized_space")))
