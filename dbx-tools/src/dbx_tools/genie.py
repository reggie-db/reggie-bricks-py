"""
Databricks Genie API client for interacting with Genie conversations and messages.

This module provides a high-level interface for working with Databricks Genie, an AI
assistant feature that can answer questions and generate SQL queries. It supports
creating conversations, sending messages, and streaming responses as Genie processes
requests.

Two flavors are exposed:


* :class:`GenieConversation` - asynchronous client that talks to the Genie REST
  API directly via :func:`dbx_tools.clients.api`. Its :meth:`GenieConversation.chat`
  method returns an async iterator of :class:`GenieResponse` updates so callers
  can grab generated SQL the moment it is published. When used as an async
  context manager the conversation is deleted on exit.
* :class:`GenieService` - synchronous wrapper around the Databricks SDK Genie client.

In addition, :class:`GenieResponse` wraps a Genie message and exposes the
message's typed query attachments (each carrying the SQL string + a
human-readable description) via :meth:`GenieResponse.query_attachments`,
:class:`GenieMessageError` is the exception raised when a Genie message
reaches ``FAILED``, and :class:`GenieSpaceExt` extends the SDK's
``GenieSpace`` dataclass with the parsed ``serialized_space`` payload.
"""

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Iterable, Mapping

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.dashboards import (
    GenieAttachment,
    GenieMessage,
    GenieQueryAttachment,
    GenieSuggestedQuestionsAttachment,
    MessageError,
    MessageStatus,
    TextAttachment,
)
from databricks.sdk.service.dashboards import GenieSpace as _SdkGenieSpace
from dbx_core import objects, strs
from lfp_logging import logs

from dbx_tools import clients

LOG = logs.logger()

_DEFAULT_POLL_INTERVAL_SECONDS = 0.1


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
      ``404`` triggers a transparent recreate when ``strict=False`` (the
      default), or propagates the error when ``strict=True``.
    * Subsequent :meth:`chat` calls append to the same conversation via
      ``POST .../conversations/{conversation_id}/messages``.
    * On async context exit the conversation (whether auto-created or
      user-supplied) is deleted via :meth:`delete` and ``conversation_id`` is
      cleared. The httpx client is **not** closed here; the caller owns its
      lifecycle (or relies on the cached default's process-lifetime pool).

    The ``strict`` flag also affects :meth:`delete`: when False, a 404 from
    the DELETE call is logged at WARNING and treated as success; when True it
    propagates as an HTTP error.
    """

    def __init__(
        self,
        space_id: str,
        client: WorkspaceClient | httpx.AsyncClient | None = None,
        conversation_id: str | None = None,
        poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
        strict: bool = False,
    ):
        """Initialize a GenieConversation.

        Args:
            space_id: Genie space ID (from the Databricks UI or API).
            client: Optional pre-built authenticated httpx client. When
                omitted, the cached default client returned by
                :func:`dbx_tools.clients.api` is used. The instance never
                closes the client; the caller (or the cached default) owns
                its lifecycle.
            conversation_id: Optional existing conversation ID to attach to.
                When omitted, the first :meth:`chat` call starts a new
                conversation. When supplied, the ID is validated on first use
                and (when ``strict=False``) silently recreated if the server
                returns ``404``.
            poll_interval_seconds: Delay between message-status polls inside
                :meth:`_responses`.
            strict: When ``False`` (default), a ``404`` from either the
                conversation-validation probe or the DELETE call is treated
                as "already gone" and logged at WARNING. When ``True``, both
                paths raise the underlying HTTP error so the caller can
                react to the missing conversation explicitly.
        """
        self.space_id = space_id
        if isinstance(client, WorkspaceClient):
            self.api_client = clients.api(client)
        elif client is None:
            self.api_client = clients.api()
        else:
            self.api_client = client
        self.conversation_id = conversation_id
        self._poll_interval_seconds = poll_interval_seconds
        self._strict = strict
        # ``_conversation_id_valid`` flips to True only after a server-side
        # GET (existing id) or POST (new conversation) succeeds, so subsequent
        # chats can skip re-entering the lock once initialization is settled.
        self._conversation_id_valid = False
        self._conversation_id_lock: asyncio.Lock = asyncio.Lock()

    async def __aenter__(self) -> "GenieConversation":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Delete the attached conversation when the ``async with`` block exits.

        Delegates to :meth:`delete`, which is a no-op when no conversation
        has been attached. With ``strict=False`` (default) a 404 from the
        DELETE call is swallowed inside :meth:`delete`; with ``strict=True``
        it propagates and aborts cleanup.
        """
        await self.delete()

    async def chat(self, message: str) -> tuple[str, AsyncIterator["GenieResponse"]]:
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
              status raises :class:`GenieMessageError` from the iterator with
              the conversation/message ids and the underlying
              :class:`MessageError` payload attached.
        """

        message_id = None
        if not self._conversation_id_valid:
            async with self._conversation_id_lock:
                if not self._conversation_id_valid:
                    if self.conversation_id:
                        # Validate a caller-supplied ID with a cheap probe; treat
                        # 404 as "stale" and fall through to recreate.
                        resp = await self._api_request(
                            "GET",
                            f"conversations/{self.conversation_id}/messages?page_size=0",
                        )
                        if not self._strict and resp.status_code == 404:
                            LOG.warning(
                                f"Conversation {self.conversation_id} not found, recreating:{resp}"
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
                        self.conversation_id = payload["conversation_id"]
                        self._conversation_id_valid = True
                        LOG.info(f"Conversation {self.conversation_id} created")

        if not message_id:
            payload = await self._api_call(
                "POST",
                f"conversations/{self.conversation_id}/messages",
                GenieConversation._message_body(message),
            )
            message_id = GenieConversation._message_id(payload)
        return message_id, self._responses(message_id)

    async def delete(self) -> bool:
        """Delete the attached conversation via the Genie REST API.

        No-ops (returns ``False``) when no conversation is attached. With
        ``strict=False`` (the instance default), a ``404`` from the server is
        treated as "already gone" and logged at WARNING; with ``strict=True``
        the 404 propagates via ``raise_for_status``. Any other non-2xx always
        propagates regardless of ``strict``.

        After a successful delete (or a swallowed 404) the local
        ``conversation_id`` is cleared so a subsequent :meth:`chat` call will
        start a fresh conversation. When the call raises (strict 404 or
        other error), ``conversation_id`` is **not** cleared. The
        double-checked-lock pattern ensures concurrent callers do not race
        on the DELETE.

        Returns:
            ``True`` when the server confirmed the delete (2xx), ``False``
            otherwise (no conversation attached, or non-strict 404).
        """
        deleted = False
        if self.conversation_id:
            async with self._conversation_id_lock:
                if self.conversation_id:
                    resp = await self._api_request(
                        "DELETE", f"conversations/{self.conversation_id}"
                    )
                    if not self._strict and resp.status_code == 404:
                        LOG.warning(
                            f"Conversation {self.conversation_id} not found, skipping delete:{resp}"
                        )
                    else:
                        resp.raise_for_status()
                        deleted = True
                        LOG.info(f"Conversation {self.conversation_id} deleted")
                    self.conversation_id = None
                    self._conversation_id_valid = False

        return deleted

    async def _responses(self, message_id: str) -> AsyncIterator["GenieResponse"]:
        """Poll a Genie message and yield :class:`GenieResponse` updates.

        Yields only when the wrapped message hash changes (deduplicated). The
        loop stops cleanly on :attr:`MessageStatus.COMPLETED` and raises
        :class:`GenieMessageError` on :attr:`MessageStatus.FAILED`.
        """
        message_url = f"conversations/{self.conversation_id}/messages/{message_id}"
        current_response: GenieResponse | None = None
        while True:
            payload = await self._api_call("GET", message_url)
            response = GenieResponse(GenieMessage.from_dict(payload))
            if current_response is None or current_response.hash != response.hash:
                current_response = response
                LOG.debug(f"Genie response: {response.status_display}")
                yield response
            message_status = response.message.status if response.message else None
            if message_status == MessageStatus.FAILED:
                raise GenieMessageError(
                    self.conversation_id, message_id, response.message.error
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
        resp = await self.api_client.request(method, request_path, json=json_body)
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
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _message_body(message: str) -> Mapping[str, Any]:
        return {"content": message}

    @staticmethod
    def _message_id(payload: Mapping[str, Any]) -> str:
        return payload.get("message_id") or payload["id"]


class GenieService:
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

        Returns as soon as Genie acknowledges the message (typically with a
        ``PENDING`` / ``SUBMITTED`` status). It does **not** wait for Genie
        to finish processing the request - poll :meth:`get_message` (or use
        :meth:`chat`) if you need to observe progression to ``COMPLETED``.

        Args:
            conversation_id: ID of the conversation to add the message to
            content: Message content to send to Genie

        Returns:
            ID of the freshly created message
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

    Exposes the message's typed query attachments (SDK
    :class:`GenieQueryAttachment` payloads, each carrying both the SQL string
    and the human-readable description) via :meth:`query_attachments`, plus a
    cached :attr:`hash` for detecting when message content changes between
    polls and a :attr:`status_display` label for surfacing status to a UI.
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

    def attachments(self) -> Iterable[GenieAttachment]:
        if attachments := self.message.attachments:
            for attachment in attachments:
                yield attachment

    def text_attachments(self) -> Iterable[TextAttachment]:
        for attachment in self.attachments():
            if attachment_text := attachment.text:
                yield attachment_text

    def suggested_questions_attachments(
        self,
    ) -> Iterable[GenieSuggestedQuestionsAttachment]:
        for attachment in self.attachments():
            if attachment_suggested_questions := attachment.suggested_questions:
                yield attachment_suggested_questions

    def query_attachments(self) -> Iterable[GenieQueryAttachment]:
        """
        Yield each :class:`GenieQueryAttachment` produced by Genie.

        Iterates the wrapped message's ``attachments``, yielding only the
        ``query`` payload (the SDK's ``GenieQueryAttachment``) for attachments
        that actually have one. Attachments without a query payload (e.g. pure
        text attachments) are skipped.

        Callers can read both the SQL (``qa.query``) and the human-readable
        description (``qa.description``) off each yielded attachment in a
        single pass::

            for qa in response.query_attachments():
                if sql := qa.query:
                    ...
                if desc := qa.description:
                    ...
        """
        for attachment in self.attachments():
            if attachment_query := attachment.query:
                yield attachment_query

    @property
    def status_display(self) -> str | None:
        """
        Human-readable label for the wrapped message's :class:`MessageStatus`.

        Tokenises the status enum's ``name`` (e.g. ``EXECUTING_QUERY`` ->
        ``"Executing Query"``), with the special case that any token equal
        to ``"ai"`` (case-insensitively) is uppercased so ``ASKING_AI`` ->
        ``"Asking AI"``. Unknown / new statuses get the same treatment so
        the UI never blanks out when the SDK adds a new value. Falls back to
        ``str(status)`` when tokenisation yields an empty string. Returns
        ``None`` when the underlying message has no status set.
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

    def __str__(self) -> str:
        return f"GenieResponse: hash:{self.hash} message:{self.message}"


class GenieMessageError(Exception):
    """Raised when a Genie message reaches :attr:`MessageStatus.FAILED`.

    Surfaced by :meth:`GenieConversation.chat`'s response iterator (via
    :meth:`GenieConversation._responses`). Carries the conversation and
    message ids plus the SDK's :class:`MessageError` payload (when present)
    so callers can inspect ``error.error`` / ``error.type`` for a granular
    failure reason rather than a generic ``FAILED`` status.

    Attributes:
        conversation_id: ID of the conversation that produced the failed message.
        message_id: ID of the message that reached ``FAILED``.
        error: SDK ``MessageError`` payload describing the failure, or ``None``
            when Genie did not include one.
    """

    def __init__(
        self,
        conversation_id: str | None,
        message_id: str | None,
        error: MessageError | None,
    ):
        self.conversation_id = conversation_id
        self.message_id = message_id
        self.error = error

        super().__init__(
            f"Conversation {conversation_id} failed (message_id={message_id}): {error}"
        )


@dataclass
class GenieSpaceExt(_SdkGenieSpace):
    """SDK ``GenieSpace`` extended with the parsed ``serialized_space`` JSON.

    The SDK exposes ``serialized_space`` as an opaque string. This subclass
    parses it (when present) into ``data`` so callers can inspect the space's
    layout / components without re-deserialising on every access.
    """

    #: Parsed contents of ``serialized_space`` (empty dict when the field is
    #: missing or the space was loaded without ``include_serialized_space=True``).
    data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "GenieSpaceExt":
        """Deserialize a Genie space dict, also parsing ``serialized_space`` JSON.

        Mirrors the SDK's ``GenieSpace.from_dict`` and additionally decodes
        the ``serialized_space`` string into a dict on the resulting
        instance's ``data`` attribute.
        """
        genie_space = _SdkGenieSpace.from_dict(d)
        serialized_space: str | None = d.get("serialized_space", None)
        return cls(
            data=json.loads(serialized_space) if serialized_space else {},
            **genie_space.__dict__,
        )


def space(
    identifier: str,
    include_serialized_space: bool = False,
    page_size: int | None = None,
    workspace_client: WorkspaceClient | None = None,
) -> GenieSpaceExt:
    if not identifier:
        raise ValueError("identifier is required")
    wc = workspace_client or clients.workspace_client()
    genie = wc.genie

    def _get_space(space_id: str) -> GenieSpaceExt | None:
        try:
            space = genie.get_space(
                space_id=space_id, include_serialized_space=include_serialized_space
            )
            if space:
                return GenieSpaceExt.from_dict(space.as_dict())
        except (NotFound, PermissionDenied):
            pass
        return None

    if space := _get_space(identifier):
        return space
    space_ids: set[str] = set()
    space_id: str | None = None
    page_token: str | None = None
    while True:
        list_spaces_resp = genie.list_spaces(page_size=page_size, page_token=page_token)
        space_id_len = len(space_ids)
        page_token = list_spaces_resp.next_page_token
        if spaces := list_spaces_resp.spaces:
            for space in spaces:
                space_ids.add(space.space_id)
                if space.title == identifier:
                    if space_id:
                        raise ValueError(
                            f"Multiple spaces found with title {identifier}"
                        )
                    else:
                        space_id = space.space_id
        if not page_token or space_id_len == len(space_ids):
            break
    if space_id:
        if space := _get_space(space_id):
            return space
    raise ValueError(f"Space not found: {identifier}")


if __name__ == "__main__":
    genie_space = space("FuelOpt Planner Curated", include_serialized_space=True)
    print(genie_space.warehouse_id)
    print(genie_space.description)
