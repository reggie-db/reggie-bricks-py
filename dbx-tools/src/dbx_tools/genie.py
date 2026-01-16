"""
Databricks Genie API client for interacting with Genie conversations and messages.

This module provides a high-level interface for working with Databricks Genie, an AI
assistant feature that can answer questions and generate SQL queries. It supports
creating conversations, sending messages, and streaming responses as Genie processes
requests.
"""

import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Iterable

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieMessage, GenieSpace, MessageStatus
from lfp_logging import logs
from reggie_core import objects

from dbx_tools import clients, configs

LOG = logs.logger()


class Service:
    """
    Client for interacting with Databricks Genie conversations and messages.

    Provides methods to create conversations, send messages, retrieve responses,
    and stream updates as Genie processes requests. Uses the Databricks SDK's
    native Genie interface for all operations.
    """

    def __init__(self, workspace_client: WorkspaceClient, genie_space_id: str):
        """
        Initialize a Genie service client.

        Args:
            workspace_client: Databricks WorkspaceClient instance for API access
            genie_space_id: Genie space ID obtained from the Databricks UI or API
        """
        self.genie = workspace_client.genie
        self.space_id = genie_space_id

    def get_space(self) -> "GenieSpaceExt":
        """
        Fetch details about the Genie space.

        Returns:
            Space details object containing metadata about the Genie space
        """
        return self.genie.get_space(self.space_id)

    def create_conversation(self, content: str) -> GenieMessage:
        """
        Start a new conversation in the Genie space.

        Creates a new conversation thread and sends an initial message. Waits for
        Genie to process the message before returning.

        Args:
            content: Initial message content to send to Genie

        Returns:
            GenieMessage object containing the conversation ID and initial response
        """
        return self.genie.start_conversation_and_wait(
            space_id=self.space_id, content=content
        )

    def create_message(self, conversation_id: str, content: str) -> str:
        """
        Create a new message in an existing conversation.

        Sends a message to Genie within a conversation thread and waits for the
        message to be created.

        Args:
            conversation_id: ID of the conversation to add the message to
            content: Message content to send to Genie

        Returns:
            ID of the created message
        """
        create_message_wait = self.genie.create_message(
            space_id=self.space_id, conversation_id=conversation_id, content=content
        )
        return create_message_wait.response.id

    def get_message(self, conversation_id: str, message_id: str) -> GenieMessage:
        """
        Retrieve a specific message from a conversation.

        Args:
            conversation_id: ID of the conversation containing the message
            message_id: ID of the message to retrieve

        Returns:
            GenieMessage object containing the message details and content
        """
        return self.genie.get_message(
            space_id=self.space_id,
            conversation_id=conversation_id,
            message_id=message_id,
        )

    def chat(self, conversation_id: str, content: str) -> Iterable["GenieResponse"]:
        """
        Send a message and stream response updates as Genie processes it.

        Creates a message in the conversation and then polls for updates, yielding
        new GenieResponse objects whenever the message content changes. Continues
        until the message status is COMPLETED or FAILED.

        Args:
            conversation_id: ID of the conversation to send the message to
            content: Message content to send to Genie

        Yields:
            GenieResponse objects representing incremental updates to Genie's response.
            Each yielded response contains the latest message state.
        """
        msg_id = self.create_message(conversation_id, content)
        current_response: GenieResponse | None = None
        while True:
            response = GenieResponse(self.get_message(conversation_id, msg_id))
            # Only yield when the response content has changed (detected via hash)
            if current_response is None or current_response.hash != response.hash:
                current_response = response
                yield response
            # Stop polling when Genie has finished processing
            response_message_status = (
                response.message.status if response.message else None
            )
            if response_message_status in (
                MessageStatus.COMPLETED,
                MessageStatus.FAILED,
            ):
                break


class GenieResponse:
    """
    Wrapper around GenieMessage that provides convenient access to response data.

    Extracts query descriptions and SQL queries from Genie message attachments and
    provides a hash property for detecting when message content changes.
    """

    _hash: str | None = None

    def __init__(self, message: GenieMessage):
        """
        Initialize a GenieResponse wrapper.

        Args:
            message: GenieMessage object from the Databricks SDK
        """
        self.message = message

    @property
    def hash(self) -> str:
        """
        Get a hash of the message content for change detection.

        Computes a SHA-256 hash of the message object to detect when Genie has
        updated the response content. Cached after first computation.

        Returns:
            Hexadecimal hash string of the message content
        """
        if self._hash is None:
            self._hash = objects.hash(self.message).hexdigest()
        return self._hash

    def descriptions(self) -> Iterable[str]:
        """
        Extract query descriptions from message attachments.

        Yields description strings from query attachments in the Genie response.
        These descriptions explain what each query does.

        Yields:
            Description strings for queries found in message attachments
        """
        return self._attachment_values("query", "description")

    def queries(self) -> Iterable[str]:
        """
        Extract SQL queries from message attachments.

        Yields SQL query strings from query attachments in the Genie response.
        These are the actual SQL statements that Genie generated.

        Yields:
            SQL query strings found in message attachments
        """
        return self._attachment_values("query", "query")

    @property
    def status_display(self) -> str | None:
        status = self.message.status
        if status:
            match status:
                case MessageStatus.FETCHING_METADATA:
                    return "Fetching metadata"
                case MessageStatus.FILTERING_CONTEXT:
                    return "Filtering context"
                case MessageStatus.ASKING_AI:
                    return "Asking AI"
                case MessageStatus.PENDING_WAREHOUSE:
                    return "Pending warehouse"
                case MessageStatus.EXECUTING_QUERY:
                    return "Executing query"
                case MessageStatus.FAILED:
                    return "Failed"
                case MessageStatus.COMPLETED:
                    return "Completed"
                case MessageStatus.SUBMITTED:
                    return "Submitted"
                case MessageStatus.QUERY_RESULT_EXPIRED:
                    return "Query result expired"
                case MessageStatus.CANCELLED:
                    return "Cancelled"
        if status:
            cleaned = "".join(ch if ch.isalnum() else " " for ch in str(status))
            cleaned = " ".join(cleaned.split())
            return cleaned or str(status)
        return None

    def _attachment_values(self, *keys: str) -> Iterable[Any]:
        """
        Extract nested values from message attachments by traversing attribute keys.

        Navigates through attachment objects using the provided key path and yields
        values found at the end of the path. Skips attachments that don't have the
        full key path.

        Args:
            *keys: Sequence of attribute names to traverse (e.g., "query", "description")

        Yields:
            Values found at the end of the key path in attachments
        """
        if self.message.attachments:
            for attachment in (a for a in self.message.attachments if a):
                value = attachment
                # Traverse the key path through nested attributes
                for key in keys:
                    value = getattr(value, key, None)
                    if not value:
                        break
                if value:
                    yield value

    def __str__(self) -> str:
        return f"GenieResponse: hash:{self.hash} queries:{list(self.queries())} descriptions:{list(self.descriptions())} message:{self.message}"


@dataclass
class GenieSpaceExt(GenieSpace):
    data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "GenieSpaceExt":
        """Deserializes the GenieSpace from a dictionary."""
        genie_space = GenieSpace.from_dict(d)
        serialized_space: str | None = d.get("serialized_space", None)
        return cls(
            data=json.loads(serialized_space) if serialized_space else {},
            **genie_space.__dict__,
        )


def main() -> None:
    """
    Interactive command-line interface for chatting with Genie.

    Reads requests from stdin and sends them to Genie, displaying responses
    and executing SQL queries as they are generated. Creates a conversation on
    the first request and reuses it for subsequent messages.
    """
    config = configs.get()
    service = Service(clients.workspace_client(config), GENIE_SPACE_ID)
    spark = clients.spark(config)
    conversation_id: str | None = None
    current_query: str | None = None
    while True:
        LOG.info("enter request")
        request = sys.stdin.readline().strip()
        if not request:
            break
        # Create conversation on first request
        if conversation_id is None:
            conversation_id = service.create_conversation(
                "Questions about image detections"
            ).conversation_id
        # Stream responses as Genie processes the request
        for response in service.chat(conversation_id, request):
            LOG.info(f"msg:{response.message}")
            # Display query descriptions
            for v in response.descriptions:
                LOG.info(f"description:{v}")
            # Execute new queries as they appear
            for query in response.queries:
                if current_query != query:
                    current_query = query
                    spark.sql(query).show()

        LOG.info("done\n")


if __name__ == "__main__":
    os.environ["DATABRICKS_CONFIG_PROFILE"] = "E2-DOGFOOD"
    GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f09d59bdff163e88db9bc395a1e08e")
    main()
