import os

from reggie_tools import clients, genie

if __name__ == "__main__":
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "FIELD-ENG-EAST")
    genie_space_id = "01f0cfa53c571bbb9b36f0e14a4e408d"
    wc = clients.workspace_client()
    genie_service = genie.Service(wc, genie_space_id)
    conv = genie_service.create_conversation("Answer questions about invoices")
    resps = genie_service.chat(
        conv.conversation_id, "find apple invoices that do not contain serial numbers"
    )
    for resp in resps:
        print(resp)
        queries = list(resp.queries)
        if queries:
            for query in queries:
                print(query)
            break
