"""CLI smoke test for the async ``genie.space`` / :class:`GenieSpace` helper.

Exercises a multi-turn flow against a real Genie space:

1. Open a :class:`GenieSpace` with ``auto_delete_conversation=True`` so the
   conversation is cleaned up on context exit.
2. Stream the response to an initial chat.
3. Continue the same conversation with a follow-up chat (sharing a query
   dedup set so only newly produced SQL is printed).

Run with::

    uv run --project dbx-tools python dbx-tools/tests/genie_space_async_test.py
"""

import asyncio
import json

from dbx_tools import clients, warehouses
from dbx_tools.genie import GenieConversation


async def main() -> None:
    wc = clients.workspace_client()
    spaces = wc.genie.list_spaces(page_size=100).spaces
    assert spaces is not None
    space_id = None
    for space in spaces:
        print(space.title)
        if "curat" in space.title.lower():
            space_id = space.space_id
            break
    assert space_id is not None

    async with GenieConversation(
        space_id, conversation_id="12312327405612148043847"
    ) as conv:
        while True:
            message = input("Enter a message: ").strip()
            if not message:
                break
            statement_ids = set()
            message_id, responses = await conv.chat(message)
            print(f"message_id: {message_id}")
            print("--------------------------------")
            async for response in responses:
                print(f"status: {response.status_display}")
                print(json.dumps(response.message.as_dict(), indent=2))
                for query_attachment in response.query_attachments():
                    if (
                        statement_id := query_attachment.statement_id
                    ) and statement_id not in statement_ids:
                        statement_ids.add(statement_id)
                        print(f"statement_id: {statement_id}")
                        statement_resp = await warehouses.statement_response_async(
                            statement_id, client=conv.api_client
                        )
                        print(statement_resp)

            print("--------------------------------")


if __name__ == "__main__":
    asyncio.run(main())
