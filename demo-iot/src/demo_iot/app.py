import asyncio
import contextvars
import json
import os

import uvicorn
from dbx_tools import configs
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from lfp_logging import logs
from sqlalchemy import func
from sqlalchemy.orm import Session
from starlette.middleware.cors import CORSMiddleware

from demo_iot import image_service
from demo_iot.apis import APIImplementation
from demo_iot.database import get_engine
from demo_iot.models import Detection, Frame
from demo_iot_generated.main import GeneratedRouter

LOG = logs.logger()


def main():
    LOG.debug("Starting fastapi app")
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
    async def stream_detections(
        request: Request,
        interval: float = Query(default=1.0, ge=0.1),
        limit: int = Query(default=100, ge=1),
    ):
        """
        SSE endpoint that polls PostgreSQL for new detections and frames.

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
                ingest_timestamp: str (ISO)
                update_timestamp: str (ISO)
                stream_id: str
                fps: int
                scale: int
        """

        async def generate():
            from datetime import datetime, timedelta, timezone

            nonlocal stream_states

            LOG.info(f"Starting PG poll for {len(stream_states)} streams")

            while True:
                if await request.is_disconnected():
                    LOG.info("SSE client disconnected")
                    break

                try:
                    events = []

                    def poll():
                        nonlocal stream_states
                        with Session(get_engine()) as db:
                            # 1. Discover any new stream_ids that appeared since last poll
                            known_streams = set(stream_states.keys())
                            all_streams_query = (
                                db.query(Frame.stream_id).distinct().all()
                            )
                            for (sid,) in all_streams_query:
                                if sid and sid not in known_streams:
                                    # For new streams, find their latest detection and go back 1 day
                                    latest_ts = (
                                        db.query(
                                            func.max(Detection.detection_timestamp)
                                        )
                                        .join(Frame, Detection.frame_id == Frame.id)
                                        .filter(Frame.stream_id == sid)
                                        .filter(Detection.label.isnot(None))
                                        .scalar()
                                    )
                                    stream_states[sid] = {
                                        "last_ts": (latest_ts - timedelta(days=1))
                                        if latest_ts
                                        else datetime.fromtimestamp(0, tz=timezone.utc),
                                        "last_ids": set(),
                                    }
                                    LOG.info(
                                        f"Discovered new stream: {sid}, starting from {stream_states[sid]['last_ts']}"
                                    )

                            # 2. Poll each stream individually
                            total_new_dets = 0
                            total_new_frames = 0

                            for sid, state in stream_states.items():
                                det_query = (
                                    db.query(Detection)
                                    .join(Frame, Detection.frame_id == Frame.id)
                                    .filter(Frame.stream_id == sid)
                                    .filter(
                                        Detection.detection_timestamp
                                        >= state["last_ts"]
                                    )
                                    .filter(Detection.label.isnot(None))
                                    .order_by(
                                        Detection.detection_timestamp.asc(),
                                        Detection.id.asc(),
                                    )
                                    .limit(limit)
                                )
                                new_detections = det_query.all()

                                # Filter duplicates for the current boundary timestamp
                                new_detections = [
                                    d
                                    for d in new_detections
                                    if not (
                                        d.detection_timestamp == state["last_ts"]
                                        and d.id in state["last_ids"]
                                    )
                                ]

                                if not new_detections:
                                    continue

                                # Get associated frames
                                frame_ids = {det.frame_id for det in new_detections}
                                frames_map = {
                                    f.id: f
                                    for f in db.query(Frame)
                                    .filter(Frame.id.in_(frame_ids))
                                    .all()
                                }

                                current_ts_ids = set()
                                for det in new_detections:
                                    events.append(
                                        {
                                            "type": "detection",
                                            "data": {
                                                "id": det.id,
                                                "frame_id": str(det.frame_id),
                                                "timestamp": det.detection_timestamp.isoformat(),
                                                "label": det.label,
                                                "class_id": det.class_id,
                                                "confidence": det.confidence,
                                                "bbox": [
                                                    det.x1,
                                                    det.y1,
                                                    det.x2,
                                                    det.y2,
                                                ],
                                                "stream_id": sid,
                                            },
                                        }
                                    )

                                    frame = frames_map.get(det.frame_id)
                                    if frame:
                                        events.append(
                                            {
                                                "type": "frame",
                                                "data": {
                                                    "id": str(frame.id),
                                                    "timestamp": frame.timestamp.isoformat()
                                                    if frame.timestamp
                                                    else None,
                                                    "ingest_timestamp": frame.ingest_timestamp.isoformat()
                                                    if frame.ingest_timestamp
                                                    else None,
                                                    "update_timestamp": frame.update_timestamp.isoformat()
                                                    if frame.update_timestamp
                                                    else None,
                                                    "stream_id": frame.stream_id,
                                                    "fps": frame.fps,
                                                    "scale": frame.scale,
                                                },
                                            }
                                        )
                                        total_new_frames += 1

                                    # Update state for this stream
                                    if det.detection_timestamp > state["last_ts"]:
                                        state["last_ts"] = det.detection_timestamp
                                        current_ts_ids = {det.id}
                                    else:
                                        current_ts_ids.add(det.id)

                                state["last_ids"] = current_ts_ids
                                total_new_dets += len(new_detections)

                                # LOG each time a specific stream updates
                                LOG.info(
                                    f"Stream '{sid}' updated: {len(new_detections)} new detections"
                                )

                            return total_new_dets, total_new_frames

                    loop = asyncio.get_event_loop()
                    det_count, frame_count = await loop.run_in_executor(None, poll)

                    for event in events:
                        yield f"data: {json.dumps(event)}\n\n"

                    if det_count > 0 or frame_count > 0:
                        LOG.info(
                            f"Emitted {det_count} detections and {frame_count} frames across streams"
                        )

                except asyncio.CancelledError:
                    LOG.info("SSE stream cancelled by client")
                    break
                except Exception as e:
                    LOG.error(f"Error polling PG: {e}")
                    yield f"data: {json.dumps({'error': str(e)})}\n\n"

                await asyncio.sleep(interval)

        # Initial state: find starting timestamps per stream (run once per request)
        def get_initial_state():
            from datetime import timedelta

            with Session(get_engine()) as db:
                # Get the latest detection timestamp for each stream_id
                latest_per_stream = (
                    db.query(Frame.stream_id, func.max(Detection.detection_timestamp))
                    .join(Detection, Detection.frame_id == Frame.id)
                    .filter(Detection.label.isnot(None))
                    .group_by(Frame.stream_id)
                    .all()
                )

                # Map stream_id -> last_emitted_ts (latest - 1 day)
                stream_states = {}
                for stream_id, latest_ts in latest_per_stream:
                    if latest_ts:
                        stream_states[stream_id] = {
                            "last_ts": latest_ts - timedelta(days=1),
                            "last_ids": set(),
                        }

                return stream_states

        loop = asyncio.get_event_loop()
        stream_states = await loop.run_in_executor(None, get_initial_state)

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

    if "loop_factory" not in asyncio.run.__code__.co_varnames:
        uvicorn.server.asyncio_run = lambda coro, **_: asyncio.run(coro)
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    main()
