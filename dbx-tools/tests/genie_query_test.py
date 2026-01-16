import os

from dbx_tools import clients, genie
from dbx_tools.genie import GenieResponse


def _attachment_ids(resp: GenieResponse) -> list[str]:
    attachment_ids = []
    if resp.message:
        for attachment in resp.message.attachments:
            if attachment.attachment_id:
                attachment_ids.append(attachment.attachment_id)
    return attachment_ids


if __name__ == "__main__":
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "FIELD-ENG-EAST")
    genie_space_id = "01f0cfa53c571bbb9b36f0e14a4e408d"
    wc = clients.workspace_client()
    genie_service = genie.Service(wc, genie_space_id)
    conv = genie_service.create_conversation("Answer questions about invoices")
    resps = genie_service.chat(conv.conversation_id, "sum all invoice totals")
    attachment_id_map = {}

    for resp in resps:
        print(resp)

        queries = list(resp.queries())
        if queries:
            for query in queries:
                print(query)
        if resp.message:
            attachment_ids = attachment_id_map.setdefault(resp.message.message_id, [])
            for attachment_id in _attachment_ids(resp):
                if attachment_id in attachment_ids:
                    continue
                else:
                    attachment_ids.append(attachment_id)
                    print(attachment_id)

    for message_id, attachment_ids in attachment_id_map.items():
        for attachment_id in attachment_ids:
            try:
                execute_resp: dict = wc.genie._api.do(
                    "POST",
                    f"/api/2.0/genie/spaces/{genie_space_id}/conversations/{conv.conversation_id}/messages/{message_id}/attachments/{attachment_id}/execute-query",
                )
                print(execute_resp)
                message_resp: dict = wc.genie._api.do(
                    "GET",
                    f"/api/2.0/genie/spaces/{genie_space_id}/conversations/{conv.conversation_id}/messages/{message_id}/attachments/{attachment_id}/query-result",
                )
                print(message_resp)
            except Exception:
                print(f"attachment message not found:{attachment_id}")
