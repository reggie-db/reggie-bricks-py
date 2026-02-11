import asyncio
import contextvars
import json
import os
import re

import uvicorn
from dbx_tools import configs
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from lfp_logging import logs
from sqlalchemy import text
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from demo_iot import image_service
from demo_iot.apis import APIImplementation
from demo_iot.database import get_engine
from demo_iot.frame_api import router as frame_api_router
from demo_iot.image_api import router as image_api_router
from demo_iot_generated.main import GeneratedRouter

# This module hosts the demo FastAPI application, including an SSE endpoint that
# streams detections polled from the database.
LOG = logs.logger()

_STREAM_ID_SUFFIX_NUMBER_RE = re.compile(r"_\d+$")


def _is_allowed_stream_id(stream_id: str) -> bool:
    """
    Return True if a stream id should be included in SSE.

    Exclude stream ids that end with a numeric suffix like `stream01_54`.
    Keep stream ids that do not end with `_<digits>`, like `stream01`.
    """

    return not bool(_STREAM_ID_SUFFIX_NUMBER_RE.search(stream_id))


def main():
    LOG.debug("Starting fastapi app")

    app = FastAPI()
    current_request_ctx = contextvars.ContextVar("current_request")
    api = APIImplementation(configs.get(), lambda: current_request_ctx.get(None))
    app.include_router(GeneratedRouter(api).router)
    app.include_router(frame_api_router)
    app.include_router(image_api_router)

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
    async def stream_detections(
        request: Request,
        interval: float = Query(default=1.0, ge=0.1),
        limit: int = Query(
            default=100,
            ge=1,
            description="Maximum number of updated streams emitted per poll tick.",
        ),
    ):
        """
        SSE endpoint that emits the latest detection per stream_id.

        On connect, it emits a snapshot (latest per stream_id). After that it polls
        for any new detections and publishes an update only for stream_ids that
        changed, always emitting the current latest per stream_id.

        Responses:
            - type: "detection"
              data:
                id: int
                frame_id: str (UUID)
                timestamp: str (ISO)
                label: str
                class_id: int
                confidence: float
                bbox: [x1, y1, x2, y2]
            - type: "frame"
              data:
                id: str (UUID)
                timestamp: str (ISO)
                update_timestamp: str (ISO)
                stream_id: str
                fps: int
                scale: int
        """

        async def generate():
            from datetime import datetime, timezone

            # Global watermark across ALL streams: (timestamp, id)
            watermark_ts = datetime.fromtimestamp(0, tz=timezone.utc)
            watermark_id = 0
            initial_snapshot_sent = False

            def _latest_per_stream(*, db, stream_ids: list[str] | None) -> list[dict]:
                """
                Return SSE event payloads for the latest detection per stream_id.
                If stream_ids is provided, restrict to those stream ids.
                """

                where_streams = ""
                params: dict[str, object] = {}
                if stream_ids:
                    where_streams = "AND f.stream_id = ANY(CAST(:stream_ids AS text[]))"
                    params["stream_ids"] = stream_ids

                sql = text(
                    f"""
SELECT DISTINCT ON (f.stream_id)
    f.stream_id::text AS stream_id,
    d.id AS detection_id,
    d.frame_id::text AS frame_id,
    d.inference_timestamp AS ts,
    d.label AS label,
    d.class_id AS class_id,
    d.confidence AS confidence,
    d.x1 AS x1,
    d.y1 AS y1,
    d.x2 AS x2,
    d.y2 AS y2,
    f.timestamp AS frame_timestamp,
    f.update_timestamp AS frame_update_timestamp,
    f.fps AS fps,
    f.scale AS scale
FROM detections d
JOIN frames f ON d.frame_id = f.id
WHERE d.label IS NOT NULL
  AND f.stream_id IS NOT NULL
  {where_streams}
ORDER BY f.stream_id, d.inference_timestamp DESC, d.id DESC;
"""
                )

                rows = db.execute(sql, params).mappings().all()

                events: list[dict] = []
                for r in rows:
                    stream_id = r["stream_id"]
                    events.append(
                        {
                            "type": "detection",
                            "data": {
                                "id": int(r["detection_id"]),
                                "frame_id": str(r["frame_id"]),
                                "timestamp": r["ts"].isoformat() if r["ts"] else None,
                                "label": r["label"],
                                "class_id": r["class_id"],
                                "confidence": r["confidence"],
                                "bbox": [r["x1"], r["y1"], r["x2"], r["y2"]],
                                "stream_id": stream_id,
                            },
                        }
                    )
                    events.append(
                        {
                            "type": "frame",
                            "data": {
                                "id": str(r["frame_id"]),
                                "timestamp": r["frame_timestamp"].isoformat()
                                if r["frame_timestamp"]
                                else None,
                                "update_timestamp": r[
                                    "frame_update_timestamp"
                                ].isoformat()
                                if r["frame_update_timestamp"]
                                else None,
                                "stream_id": stream_id,
                                "fps": r["fps"],
                                "scale": r["scale"],
                            },
                        }
                    )
                return events

            def _latest_watermark(*, db, after_ts, after_id) -> tuple[datetime, int]:
                """
                Return the latest (timestamp, id) among detections newer than the given watermark.
                """

                sql = text(
                    """
SELECT d.inference_timestamp AS ts, d.id AS id
FROM detections d
WHERE d.label IS NOT NULL
  AND (d.inference_timestamp, d.id) > (CAST(:ts AS timestamptz), CAST(:id AS bigint))
ORDER BY d.inference_timestamp DESC, d.id DESC
LIMIT 1;
"""
                )
                row = (
                    db.execute(sql, {"ts": after_ts, "id": after_id}).mappings().first()
                )
                if not row:
                    return after_ts, after_id
                return row["ts"], int(row["id"])

            while True:
                if await request.is_disconnected():
                    LOG.info("SSE client disconnected")
                    break

                try:

                    def poll_once():
                        nonlocal watermark_ts, watermark_id, initial_snapshot_sent

                        with Session(get_engine()) as db:
                            # Initial snapshot: latest per stream_id
                            if not initial_snapshot_sent:
                                events = _latest_per_stream(db=db, stream_ids=None)
                                # Set watermark to the latest detection across all streams
                                watermark_ts, watermark_id = _latest_watermark(
                                    db=db,
                                    after_ts=watermark_ts,
                                    after_id=watermark_id,
                                )
                                initial_snapshot_sent = True
                                return events, 0

                            # Find which streams changed since the watermark (limit number of updated streams)
                            changed_sql = text(
                                """
WITH changes AS (
    SELECT
        f.stream_id::text AS stream_id,
        max(d.inference_timestamp) AS max_ts
    FROM detections d
    JOIN frames f ON d.frame_id = f.id
    WHERE d.label IS NOT NULL
      AND f.stream_id IS NOT NULL
      AND (d.inference_timestamp, d.id) > (CAST(:ts AS timestamptz), CAST(:id AS bigint))
    GROUP BY f.stream_id
    ORDER BY max_ts DESC
    LIMIT :limit
)
SELECT stream_id
FROM changes;
"""
                            )
                            changed_rows = db.execute(
                                changed_sql,
                                {
                                    "ts": watermark_ts,
                                    "id": watermark_id,
                                    "limit": limit,
                                },
                            ).fetchall()
                            changed_stream_ids = [
                                r[0] for r in changed_rows if r and r[0]
                            ]
                            if not changed_stream_ids:
                                return [], 0

                            # Emit current latest per changed stream_id
                            events = _latest_per_stream(
                                db=db, stream_ids=changed_stream_ids
                            )

                            # Advance watermark to the latest detection we have observed
                            watermark_ts, watermark_id = _latest_watermark(
                                db=db,
                                after_ts=watermark_ts,
                                after_id=watermark_id,
                            )

                            return events, len(changed_stream_ids)

                    loop = asyncio.get_event_loop()
                    events, updated_streams = await loop.run_in_executor(
                        None, poll_once
                    )

                    for event in events:
                        yield f"data: {json.dumps(event)}\n\n"

                    if updated_streams > 0:
                        LOG.info(
                            f"Emitted updates for {updated_streams} streams (watermark={watermark_ts.isoformat()}#{watermark_id})"
                        )

                except asyncio.CancelledError:
                    LOG.info("SSE stream cancelled by client")
                    break
                except Exception as e:
                    LOG.error(f"Error polling PG: {e}")
                    yield f"data: {json.dumps({'error': str(e)})}\n\n"

                await asyncio.sleep(interval)

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/image/{image_id}")
    async def get_image(image_id: str):
        cached = image_service.read_cache(image_id)
        if cached is not None:
            LOG.debug(f"Serving image {image_id} from cache")
            return Response(content=cached, media_type="image/jpeg")

        try:
            LOG.debug(f"Enqueuing image request for {image_id}")
            data = await image_service.enqueue(image_id)
            LOG.debug(
                f"Successfully retrieved image {image_id} (size: {len(data)} bytes)"
            )
        except HTTPException:
            raise
        except Exception as e:
            LOG.error(f"Error retrieving image {image_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

        return Response(content=data, media_type="image/jpeg")

    return app


def run() -> None:
    """
    Run the FastAPI app via uvicorn.

    Keep server startup separate from `main()` so uvicorn can import `main` as an
    application factory (`uvicorn demo_iot.app:main --factory`) without trying
    to start uvicorn recursively.
    """

    app = main()
    if "loop_factory" not in asyncio.run.__code__.co_varnames:
        uvicorn.server.asyncio_run = lambda coro, **_: asyncio.run(coro)
    uvicorn.run(app, host="0.0.0.0", port=5000)


if __name__ == "__main__":
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    run()
