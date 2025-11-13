import asyncio
import random
import uuid
from typing import Any, AsyncIterable
from urllib.parse import urlencode, urlparse

import websockets
from pydantic import BaseModel, Field


async def publisher(
    msgs: AsyncIterable["InMessage"],
    url: str = "ws://localhost:8078/socket/in",
    topic: str = "events",
    **params,
):
    ws_url = _ws_url(url, topic, **params)
    print(f"publisher:{ws_url}")

    async with websockets.connect(ws_url) as ws:
        async for msg in msgs:
            msg_json = msg.model_dump_json()
            print(f"send:{msg_json}")
            await ws.send(msg_json)  # FIXED: no text=True


async def subscribe(
    url: str = "ws://localhost:8078/socket/out",
    topic: str = "events",
    key_type: str = "string",
    offset_reset_strategy: str = "latest",
    **params,
) -> AsyncIterable["OutMessage"]:
    ws_url = _ws_url(
        url,
        topic,
        **{"offsetResetStrategy": offset_reset_strategy, "keyType": key_type, **params},
    )

    print(f"subscriber:{ws_url}")

    async with websockets.connect(ws_url) as ws:
        async for msg in ws:
            print(f"recv raw:{msg}")
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
    async def emitter():
        while True:
            await asyncio.sleep(random.uniform(1, 3))
            yield InMessage(value=FieldValue(value="cool dude"))

    async def _publish():
        await publisher(emitter())

    async def _subscribe():
        async for msg in subscribe():
            print(f"received:{msg.value.value}")

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
