"""
Databricks Genie API client for interacting with Genie conversations and messages.

This module provides a high-level interface for working with Databricks Genie, an AI
assistant feature that can answer questions and generate SQL queries. It supports
creating conversations, sending messages, and streaming responses as Genie processes
requests.

Two flavors are exposed:

* :class:`Service` - synchronous wrapper around the Databricks SDK Genie client.
* :class:`GenieConversation` - asynchronous client that talks to the Genie REST
  API directly via :func:`dbx_tools.clients.api`. Its :meth:`GenieConversation.chat`
  method returns an async iterator of :class:`GenieResponse` updates so callers
  can grab generated SQL the moment it is published. When used as an async
  context manager the conversation is deleted on exit.

In addition, :class:`GenieResponse` wraps a Genie message with helpers for
extracting SQL queries and descriptions, and :class:`GenieSpaceExt` extends the
SDK's ``GenieSpace`` dataclass with the parsed ``serialized_space`` payload.
"""

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Iterable, Mapping, cast

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import (
    GenieMessage,
    MessageStatus,
)
from databricks.sdk.service.dashboards import GenieSpace as _SdkGenieSpace
from dbx_core import objects, strs
from lfp_logging import logs

from dbx_tools import clients

LOG = logs.logger()

_DEFAULT_POLL_INTERVAL = 0.1


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

    def get_space(self) -> _SdkGenieSpace:
        """
        Fetch details about the Genie space.

        Returns the SDK's ``GenieSpace`` directly. To get the extended payload
        (with the parsed ``serialized_space`` JSON), use
        ``GenieSpaceExt.from_dict(self.genie.get_space(...).as_dict())`` instead.
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

        Computes a SHA-256 hash of the message dict with ``last_updated_timestamp``
        stripped out, so polling responses that only differ in their server-side
        timestamp do not register as a change. Cached after first computation.

        Returns:
            Hexadecimal hash string of the (timestamp-stripped) message content
        """
        if self._hash is None:
            message_data = self.message.as_dict()
            message_data.pop("last_updated_timestamp", None)
            self._hash = objects.hash(message_data).hexdigest()
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
        """
        Human-readable label for the wrapped message's :class:`MessageStatus`.

        Known statuses are mapped to fixed strings (e.g. ``EXECUTING_QUERY`` ->
        ``"Executing query"``). Unknown / new statuses fall back to a tokenised
        version of the enum's string form so the UI never blanks out when the
        SDK adds a new value. Returns ``None`` when the underlying message has
        no status set.
        """
        status = self.message.status
        if status:
            parts = strs.tokenize(status.name, capitalize=True)
            if result := " ".join(
                part.upper() if part.lower() == "ai" else part for part in parts
            ):
                return result
            else:
                return str(status)
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
class GenieSpaceExt(_SdkGenieSpace):
    data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "GenieSpaceExt":
        """Deserializes the GenieSpace from a dictionary."""
        genie_space = _SdkGenieSpace.from_dict(d)
        serialized_space: str | None = d.get("serialized_space", None)
        return cls(
            data=json.loads(serialized_space) if serialized_space else {},
            **genie_space.__dict__,
        )


class GenieConversation:
    """Async client for a Databricks Genie conversation.

    Usable in two shapes:

    * **Direct** - create the object and call :meth:`chat` (and :meth:`delete`)
      yourself. The caller is responsible for any cleanup::

          conv = GenieConversation(space_id)
          message_id, responses = await conv.chat("show invoices for 2024")
          async for response in responses:
              ...

    * **Async context manager** - the conversation is deleted (best effort)
      when the ``with`` block exits::

          async with GenieConversation(space_id) as conv:
              message_id, responses = await conv.chat("show invoices for 2024")
              async for response in responses:
                  ...
          # conversation deleted here when the with block exits

    Conversation lifecycle:

    * The conversation is lazily created on the first :meth:`chat` call (via
      ``POST /api/2.0/genie/spaces/{space_id}/start-conversation``) unless an
      existing ``conversation_id`` is provided. Provided conversation IDs are
      validated with a lightweight ``GET .../messages?page_size=0`` probe; a
      ``404`` triggers a transparent recreate.
    * Subsequent :meth:`chat` calls append to the same conversation via
      ``POST .../conversations/{conversation_id}/messages``.
    * On async context exit the conversation (whether auto-created or
      user-supplied) is deleted via :meth:`delete` and ``conversation_id`` is
      cleared. The httpx client is **not** closed here; the caller owns its
      lifecycle (or relies on the cached default's process-lifetime pool).
    """

    def __init__(
        self,
        space_id: str,
        api_client: httpx.AsyncClient | None = None,
        conversation_id: str | None = None,
        poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL,
    ):
        """Initialize a GenieConversation.

        Args:
            space_id: Genie space ID (from the Databricks UI or API).
            api_client: Optional pre-built authenticated httpx client. When
                omitted, the cached default client returned by
                :func:`dbx_tools.clients.api` is used. The instance never
                closes the client; the caller (or the cached default) owns
                its lifecycle.
            conversation_id: Optional existing conversation ID to attach to.
                When omitted, the first :meth:`chat` call starts a new
                conversation. When supplied, the ID is validated on first use
                and silently recreated if the server returns ``404``.
            poll_interval_seconds: Delay between message-status polls inside
                :meth:`_responses`.
        """
        self.space_id = space_id
        self._api_client = api_client or clients.api()
        self._conversation_id = conversation_id
        self._poll_interval_seconds = poll_interval_seconds
        # ``_conversation_id_valid`` flips to True only after a server-side
        # GET (existing id) or POST (new conversation) succeeds, so subsequent
        # chats can skip re-entering the lock once initialization is settled.
        self._conversation_id_valid = False
        self._conversation_id_lock: asyncio.Lock = asyncio.Lock()

    async def __aenter__(self) -> "GenieConversation":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Delete the attached conversation when the ``async with`` block exits.

        Delegates to :meth:`delete`, which is a no-op when no conversation has
        been attached. Errors raised by the DELETE call propagate (a 404 is
        already swallowed inside :meth:`delete`).
        """
        await self.delete()

    async def chat(self, message: str) -> tuple[str, AsyncIterator[GenieResponse]]:
        """Send ``message`` to Genie and return a stream of response updates.

        On the first call (when no conversation is attached) a new conversation
        is started by sending ``message`` as the conversation's opening prompt;
        subsequent calls append ``message`` to the same conversation thread.

        Returns:
            A tuple ``(message_id, responses)``:

            * ``message_id`` is the assistant message ID assigned by Genie.
            * ``responses`` is an async iterator that polls the message and
              yields a :class:`GenieResponse` whenever the underlying content
              changes (deduplicated via :attr:`GenieResponse.hash`). Iteration
              completes cleanly when the message reaches
              :attr:`MessageStatus.COMPLETED`. A :attr:`MessageStatus.FAILED`
              status raises :class:`RuntimeError` from the iterator.
        """
        conversation_id, message_id = await self._conversation(message)
        if not message_id:
            payload = await self._api_call(
                "POST",
                f"conversations/{conversation_id}/messages",
                GenieConversation._message_body(message),
            )
            message_id = GenieConversation._message_id(payload)
        return message_id, self._responses(conversation_id, message_id)

    async def delete(self) -> bool:
        """Delete the attached conversation via the Genie REST API.

        No-ops (returns ``False``) when no conversation is attached. A ``404``
        from the server is treated as "already gone" and logged at WARNING;
        any other non-2xx propagates via ``raise_for_status``.

        After a successful delete (or a 404) the local ``conversation_id`` is
        cleared so a subsequent :meth:`chat` call will start a fresh
        conversation. The double-checked-lock pattern ensures concurrent
        callers do not race on the DELETE.

        Returns:
            ``True`` when the server confirmed the delete (2xx), ``False``
            otherwise (no conversation attached, or 404).
        """
        deleted = False
        if self._conversation_id:
            async with self._conversation_id_lock:
                if self._conversation_id:
                    resp = await self._api_request(
                        "DELETE", f"conversations/{self._conversation_id}"
                    )
                    if resp.status_code == 404:
                        LOG.warning(
                            f"Conversation {self._conversation_id} not found, skipping delete:{resp}"
                        )
                    else:
                        resp.raise_for_status()
                        deleted = True
                        LOG.info(f"Conversation {self._conversation_id} deleted")
                    self._conversation_id = None
                    self._conversation_id_valid = False

        return deleted

    async def _conversation(self, message: str) -> tuple[str, str | None]:
        """Resolve (or lazily create) the conversation backing this instance.

        Implements double-checked locking on ``_conversation_id_valid`` so the
        first caller does the work and concurrent callers wait then skip.

        Returns:
            Tuple ``(conversation_id, message_id)``. ``message_id`` is non-None
            only when this call also created a brand-new conversation (the
            POST to ``start-conversation`` returns the opening assistant message
            in the same payload, so the caller can skip a follow-up POST).
        """
        message_id = None
        if not self._conversation_id_valid:
            async with self._conversation_id_lock:
                if not self._conversation_id_valid:
                    if self._conversation_id:
                        # Validate a caller-supplied ID with a cheap probe; treat
                        # 404 as "stale" and fall through to recreate.
                        resp = await self._api_request(
                            "GET",
                            f"conversations/{self._conversation_id}/messages?page_size=0",
                        )
                        if resp.status_code == 404:
                            LOG.warning(
                                f"Conversation {self._conversation_id} not found, recreating:{resp}"
                            )
                        else:
                            resp.raise_for_status()
                            self._conversation_id_valid = True
                    if not self._conversation_id_valid:
                        payload = await self._api_call(
                            "POST",
                            "start-conversation",
                            GenieConversation._message_body(message),
                        )
                        message_id = GenieConversation._message_id(payload)
                        self._conversation_id = payload["conversation_id"]
                        self._conversation_id_valid = True
                        LOG.info(f"Conversation {self._conversation_id} created")
        return cast(str, self._conversation_id), message_id

    async def _responses(
        self, conversation_id: str, message_id: str
    ) -> AsyncIterator[GenieResponse]:
        """Poll a Genie message and yield :class:`GenieResponse` updates.

        Yields only when the wrapped message hash changes (deduplicated). The
        loop stops cleanly on :attr:`MessageStatus.COMPLETED` and raises
        :class:`RuntimeError` on :attr:`MessageStatus.FAILED`.
        """
        message_url = f"conversations/{conversation_id}/messages/{message_id}"
        current_response: GenieResponse | None = None
        while True:
            payload = await self._api_call("GET", message_url)
            response = GenieResponse(GenieMessage.from_dict(payload))
            if current_response is None or current_response.hash != response.hash:
                current_response = response
                LOG.debug(f"Genie response: {response.status_display}")
                yield response
            message_status = response.message.status if response.message else None
            if MessageStatus.FAILED == message_status:
                raise RuntimeError(
                    f"Conversation {conversation_id} error - message_id:{message_id} error:{response.message.error}"
                )
            if message_status == MessageStatus.COMPLETED:
                break
            await asyncio.sleep(self._poll_interval_seconds)

    async def _api_request(
        self, method: str, path: str, json_body: Mapping[str, Any] | None = None
    ) -> httpx.Response:
        """Issue a Genie REST request and return the raw httpx response.

        ``path`` is appended to ``/api/2.0/genie/spaces/{space_id}/`` so callers
        pass conversation-relative paths only. Status checks are left to the
        caller (used by :meth:`delete` and :meth:`_conversation` which need to
        differentiate 404 from other failures).
        """
        request_path = f"/api/2.0/genie/spaces/{self.space_id}/{path}"
        resp = await self._api_client.request(method, request_path, json=json_body)
        return resp

    async def _api_call(
        self, method: str, path: str, json_body: Mapping[str, Any] | None = None
    ) -> Any:
        """Issue a Genie REST request and return the parsed JSON body.

        Convenience wrapper around :meth:`_api_request` for endpoints whose
        non-2xx responses are not expected (callers that care about status
        codes should use :meth:`_api_request` directly).
        """
        resp = await self._api_request(method, path, json_body)

        return resp.json()

    @staticmethod
    def _message_body(message: str) -> Mapping[str, Any]:
        return {"content": message}

    @staticmethod
    def _message_id(payload: Mapping[str, Any]) -> str:
        return payload.get("message_id") or payload["id"]
