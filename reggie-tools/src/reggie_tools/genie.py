import os
import sys
from typing import Iterable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieMessage, MessageStatus

from reggie_core import objects
from reggie_tools import clients, configs


class Service:
    def __init__(self, workspace_client: WorkspaceClient, genie_space_id: str):
        """
        Genie API client using Databricks SDK native Genie interface.
        Args:
            workspace_client: Databricks WorkspaceClient instance.
            genie_space_id: Genie space ID from UI or API.
        """
        self.genie = workspace_client.genie
        self.space_id = genie_space_id

    def get_space(self):
        """Fetch Genie space details."""
        return self.genie.get_space(space_id=self.space_id)

    def create_conversation(self, content: str) -> GenieMessage:
        """Start a new conversation in the Genie space."""
        return self.genie.start_conversation_and_wait(
            space_id=self.space_id,
            content=content
        )

    def create_message(self, conversation_id: str, content: str) -> str:
        """Create a new message in a conversation."""
        create_message_wait = self.genie.create_message(
            space_id=self.space_id,
            conversation_id=conversation_id,
            content=content
        )
        return create_message_wait.response.id

    def get_message(self, conversation_id: str, message_id: str) -> GenieMessage:
        """Retrieve a specific message."""
        return self.genie.get_message(
            space_id=self.space_id,
            conversation_id=conversation_id,
            message_id=message_id
        )

    def chat(self, conversation_id: str, content: str) -> Iterable["GenieResponse"]:
        msg_id = self.create_message(conversation_id, content)
        current_response: GenieResponse | None = None
        while True:
            response = GenieResponse(self.get_message(conversation_id, msg_id))
            if current_response is None or current_response.hash != response.hash:
                current_response = response
                yield response
            if response.message.status in (MessageStatus.COMPLETED, MessageStatus.FAILED):
                break


class GenieResponse:
    _hash: str | None = None

    def __init__(self, message: GenieMessage):
        self.message = message

    @property
    def hash(self) -> str:
        if self._hash is None:
            self._hash = objects.hash(self.message).hexdigest()
        return self._hash

    @property
    def descriptions(self) -> Iterable[str]:
        return self._attachment_values("query", "description")

    @property
    def queries(self) -> Iterable[str]:
        return self._attachment_values("query", "query")

    def _attachment_values(self, *keys: str):
        if self.message.attachments:
            for attachment in (a for a in self.message.attachments if a):
                value = attachment
                for key in keys:
                    value = getattr(value, key, None)
                    if not value:
                        break
                if value:
                    yield value


def main():
    config = configs.get()
    service = Service(clients.workspace_client(config), GENIE_SPACE_ID)
    spark = clients.spark(config)
    conversation_id: str | None = None
    current_query: str | None = None
    while True:
        print("enter request")
        request = sys.stdin.readline().strip()
        if not request:
            break
        if conversation_id is None:
            conversation_id = service.create_conversation("Questions about image detections").conversation_id
        for response in service.chat(conversation_id, request):
            print(f"msg:{response.message}")
            for v in response.descriptions:
                print(f"description:{v}")
            for query in response.queries:
                if current_query != query:
                    current_query = query
                    spark.sql(query).show()

        print("done\n")


if __name__ == "__main__":
    os.environ["DATABRICKS_CONFIG_PROFILE"] = "E2-DOGFOOD"
    GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f09d59bdff163e88db9bc395a1e08e")
    main()
