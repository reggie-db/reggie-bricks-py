import asyncio
import contextvars
import json
import os
from typing import Optional

import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import StreamingResponse
from kafka import TopicPartition
from lfp_logging import logs
from reggie_tools import configs
from starlette.middleware.cors import CORSMiddleware

from demo_iot.apis import APIImplementation
from demo_iot.kafka_consumer import create_consumer, get_latest_offset
from demo_iot_generated.main import GeneratedRouter

LOG = logs.logger()


def main():
    app = FastAPI()
    current_request_ctx = contextvars.ContextVar("current_request")
    api = APIImplementation(configs.get(), lambda: current_request_ctx.get(None))
    app.include_router(GeneratedRouter(api).router)

    @app.middleware("http")
    async def store_request(request: Request, call_next):
        token = current_request_ctx.set(request)
        try:
            return await call_next(request)
        finally:
            current_request_ctx.reset(token)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # or specific domains: ["https://example.com"]
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/sse/detections")
    async def stream_detections(offset: Optional[int] = Query(default=100, ge=0)):
        """
        SSE endpoint that streams Kafka records from the 'sink' topic.
        Gets the latest offset, subtracts the provided offset (default 100),
        and streams records starting from that position.
        Keeps connection open indefinitely and streams all new messages.
        """

        def generate():
            import time

            consumer = None
            try:
                topic = os.getenv("KAFKA_TOPIC", "sink")
                partition = 0

                # Get latest offset and calculate start offset
                latest_offset = get_latest_offset(topic, partition)
                start_offset = max(0, latest_offset - offset)

                LOG.info(
                    f"Streaming from topic '{topic}' partition {partition}, offset {start_offset} (latest: {latest_offset})"
                )

                # Create a new consumer for this SSE connection
                consumer = create_consumer()
                tp = TopicPartition(topic, partition)
                consumer.assign([tp])
                consumer.seek(tp, start_offset)

                # Keepalive interval (30 seconds)
                KEEPALIVE_INTERVAL = 30
                POLL_TIMEOUT_MS = 5000  # Poll every 5 seconds to check for keepalive
                last_keepalive = time.time()

                # Stream records indefinitely using polling
                while True:
                    # Poll for messages with timeout
                    msg_pack = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

                    # Send keepalive if needed
                    current_time = time.time()
                    if current_time - last_keepalive >= KEEPALIVE_INTERVAL:
                        yield ": keepalive\n\n"
                        last_keepalive = current_time

                    # Process any messages received
                    for tp_partition, messages in msg_pack.items():
                        for message in messages:
                            try:
                                # Parse message value as JSON if possible
                                value = message.value
                                if isinstance(value, bytes):
                                    try:
                                        value = json.loads(value.decode("utf-8"))
                                    except (json.JSONDecodeError, UnicodeDecodeError):
                                        value = value.decode("utf-8", errors="replace")

                                # Normalize score if present (handle nested structures)
                                def normalize_score(obj):
                                    """Recursively find and normalize scores > 1.0 to 0-1 range"""
                                    if isinstance(obj, dict):
                                        # Check direct score key
                                        if "score" in obj:
                                            score = obj["score"]
                                            if (
                                                isinstance(score, (int, float))
                                                and score > 1.0
                                            ):
                                                obj["score"] = score / 100.0
                                        # Recursively check nested dicts
                                        for v in obj.values():
                                            if isinstance(v, (dict, list)):
                                                normalize_score(v)
                                    elif isinstance(obj, list):
                                        for item in obj:
                                            if isinstance(item, (dict, list)):
                                                normalize_score(item)

                                normalize_score(value)
                                # Format as SSE event
                                data = {
                                    "offset": message.offset,
                                    "partition": message.partition,
                                    "timestamp": message.timestamp,
                                    "key": message.key.decode("utf-8")
                                    if message.key
                                    else None,
                                    "value": value,
                                }

                                event_data = json.dumps(data)
                                yield f"data: {event_data}\n\n"

                            except Exception as e:
                                LOG.error(
                                    f"Error processing message at offset {message.offset}: {e}"
                                )
                                error_data = json.dumps({"error": str(e)})
                                yield f"data: {error_data}\n\n"

            except Exception as e:
                LOG.error(f"Error in SSE stream: {e}")
                error_data = json.dumps({"error": str(e)})
                yield f"data: {error_data}\n\n"
            finally:
                # Clean up consumer when connection closes
                if consumer:
                    try:
                        consumer.close()
                    except Exception as e:
                        LOG.error(f"Error closing consumer: {e}")

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    if "loop_factory" not in asyncio.run.__code__.co_varnames:
        uvicorn.server.asyncio_run = lambda coro, **_: asyncio.run(coro)
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "E2-DOGFOOD")
    main()
