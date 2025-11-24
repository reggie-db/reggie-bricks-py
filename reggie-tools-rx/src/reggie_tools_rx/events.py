import asyncio
import inspect
import random
import uuid
from typing import Any, AsyncIterable, Callable
from urllib.parse import urlencode, urlparse

import aioreactive
import websockets
from expression.system import AsyncDisposable
from pydantic import BaseModel, Field
from reggie_core import logs

LOG = logs.logger(__file__)

_URL = "wss://kws.lfpconnect.io"
_TOPIC = "events"


async def publisher(
    url: str = None,
    topic: str = None,
    **params,
):
    ws_url = _ws_url(url or f"{_URL}/in", topic or _TOPIC, **params)
    subject = aioreactive.AsyncSubject()

    ws = await websockets.connect(ws_url)

    async def _ping():
        try:
            while True:
                await asyncio.sleep(10)
                await ws.ping()
        except websockets.ConnectionClosed:
            pass

    ping_task = asyncio.create_task(_ping())

    async def _pong():
        try:
            async for _ in ws:
                pass  # discard server messages
        except websockets.ConnectionClosed:
            pass

    pong_task = asyncio.create_task(_pong())

    on_close = AsyncDisposable.composite(
        AsyncDisposable.create(ws.close),
        _disposable(ping_task.cancel),
        _disposable(pong_task.cancel),
    )

    async def _send(msg: InMessage):
        msg_json = msg.model_dump_json()
        await ws.send(msg_json)

    async def _throw(_):
        await on_close.dispose_async()

    async def _close():
        await on_close.dispose_async()

    await subject.subscribe_async(_send, _throw, _close)

    return subject


async def subscribe(
    url: str = None,
    topic: str = None,
    key_type: str = "string",
    offset_reset_strategy: str = "latest",
    **params,
) -> AsyncIterable["OutMessage"]:
    ws_url = _ws_url(
        url or f"{_URL}/out",
        topic or _TOPIC,
        **{"offsetResetStrategy": offset_reset_strategy, "keyType": key_type, **params},
    )
    async with websockets.connect(ws_url) as ws:
        async for msg in ws:
            yield OutMessage.model_validate_json(msg)


def _ws_url(url: str, topic: str, **params):
    u = urlparse(url)

    scheme = "ws" if u.scheme in ("http", "ws") else "wss"

    query_params = {"topic": topic}

    for k, v in params.items():
        if k in query_params:
            raise ValueError(f"Duplicate param:{k}")
        query_params[k] = v

    if "clientId" not in query_params:
        query_params["clientId"] = f"ws_{uuid.uuid4().hex}"

    return f"{scheme}://{u.netloc}{u.path}?{urlencode(query_params)}"


def _disposable(action: Callable[[], Any]):
    """
    Wrap a sync function so that it becomes an async disposable.
    The returned object has an aclose coroutine that runs the sync function.
    """

    async def aclose(_self):
        if action is not None:
            result = action()
            if result is not None and inspect.isawaitable(result):
                await result

    return type("AsyncDisposable", (), {"aclose": aclose})()


class Header(BaseModel):
    key: str
    value: str = ""


class FieldValue(BaseModel):
    value: Any
    format: str | None = None

    def __init__(self, value: Any, format: str | None = None, **data):
        fmt = (
            format
            if format is not None
            else "string"
            if isinstance(value, str)
            else "json"
        )
        super().__init__(value=value, format=fmt, **data)


class Message(BaseModel):
    value: FieldValue
    key: FieldValue | None = None
    headers: list[Header] | None = None

    def model_post_init(self, __context):
        if self.headers is None:
            self.headers = []


class InMessage(Message):
    messageId: str = Field(default_factory=lambda: str(uuid.uuid4()))


class OutMessage(Message):
    wsProxyMessageId: str
    topic: str
    partition: int
    offset: int
    timestamp: int


async def main():
    async def _publish():
        subject = await publisher()
        try:
            while True:
                await asyncio.sleep(random.uniform(1, 3))
                await subject.asend(InMessage(value=FieldValue(value="cool dude")))
        finally:
            await subject.aclose()

    async def _subscribe():
        async for msg in subscribe():
            LOG.info(f"received:{msg.value.value}")

    tasks = []
    if True:
        tasks.append(asyncio.create_task(_publish()))
    tasks.append(asyncio.create_task(_subscribe()))

    results = await asyncio.gather(*tasks)
    for result in results:
        if isinstance(result, Exception):
            raise result


if __name__ == "__main__":
    asyncio.run(main())
