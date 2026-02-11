import asyncio
from datetime import datetime, timedelta
from typing import Any

import pytimeparse
from fastapi import APIRouter, Query, Request
from lfp_logging import logs
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from demo_iot.database import get_async_engine
from demo_iot.date_utils import DateQuery

# Frame analytics endpoints and query utilities.
LOG = logs.logger()

router = APIRouter()


def _normalize_list(
    values: list[str] | None, lowercase: bool = False
) -> list[str] | None:
    """
    Normalize a list-like query parameter.

    - Treat None or [] as None (meaning "no filter, include all")
    - Strip whitespace
    - Drop empty strings
    """

    if values:
        for idx in reversed(range(len(values))):
            value = values[idx]
            if isinstance(value, str):
                value = value.strip()
                if lowercase:
                    value = value.lower()
            # Keep numeric values, drop empty strings and None-like values.
            if not isinstance(value, (int, float)) and not value:
                values.pop(idx)
    return values or None


class FrameCoverageBucket(BaseModel):
    """
    Coverage summary for a store/device/stream within a time bucket.

    This is the direct shape of the SQL query output, with minor type coercions
    for JSON compatibility.
    """

    # NOTE: `store_id` is commonly an integer in the backing DB, but can also be
    # stored as text depending on ingestion/source system.
    store_id: int | str | None = Field(default=None, description="Store identifier")
    device_name: str | None = Field(default=None, description="Device name")
    stream_id: str | None = Field(default=None, description="Stream identifier")
    label: str = Field(description="Detection label (stored lowercase)")
    interval_start: datetime = Field(description="Interval start timestamp")
    interval_end: datetime = Field(description="Interval end timestamp")
    avg_coverage: float = Field(description="Average coverage in bucket (0.0 to 1.0)")
    avg_confidence: float = Field(
        description="Average detection confidence in bucket (0.0 to 1.0)"
    )
    total_frames: int = Field(
        description="Total frames in bucket before deviation filter"
    )


class DetectionBBox(BaseModel):
    """
    Bounding box for a detection within a frame.
    """

    label: str | None = Field(default=None, description="Detection label")
    class_id: int | None = Field(default=None, description="Detection class id")
    confidence: float = Field(description="Detection confidence (0.0 to 1.0)")
    x1: float = Field(description="Bounding box left x")
    y1: float = Field(description="Bounding box top y")
    x2: float = Field(description="Bounding box right x")
    y2: float = Field(description="Bounding box bottom y")


class FrameDetectionsBBoxResponse(BaseModel):
    """
    Grouped detection bounding boxes for a single frame.
    """

    frame_id: str = Field(description="Frame id (UUID as string)")
    boxes: list[DetectionBBox] = Field(default_factory=list)


class FrameDetectionsBBoxRequest(BaseModel):
    """
    Request payload for detection bounding boxes for specific frames.
    """

    frame_ids: list[str] = Field(
        min_length=1, description="Frame ids (UUID strings) to query"
    )
    label: str | None = Field(
        default=None,
        description="Optional label filter. If omitted, include all labels.",
    )
    min_confidence: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Minimum confidence threshold (0.0 to 1.0)",
    )


_FRAME_COVERAGE_SQL = text(
    """
WITH params AS NOT MATERIALIZED (
    SELECT
        CAST(:endpoint_name AS text[]) AS endpoint_names,
        CAST(:label AS text[]) AS labels,
        CAST(:min_confidence AS float8) AS min_confidence,
        CAST(:bucket_interval AS interval) AS bucket_interval,
        CAST(:max_deviation AS float8) AS max_deviation,
        CAST(:start_ts AS timestamptz) AS start_ts,
        CAST(:end_ts AS timestamptz) AS end_ts,
        CAST(:store_id AS text[]) AS store_ids,
        CAST(:device_name AS text[]) AS device_names
),

/* ---------- frame filter FIRST (critical for planner) ---------- */
frames_filtered AS (
    SELECT
        f.id,
        f.store_id,
        f.device_name,
        f.stream_id,
        f.timestamp,
        f.frame_area
    FROM frames f, params p
    WHERE f.scale IS NOT NULL
      AND f.timestamp IS NOT NULL
      AND (p.start_ts IS NULL OR f.timestamp >= p.start_ts)
      AND (p.end_ts   IS NULL OR f.timestamp <= p.end_ts)
      AND (p.store_ids IS NULL OR f.store_id::text = ANY(p.store_ids))
      AND (
            p.device_names IS NULL
         OR (f.device_name IS NOT NULL AND
             EXISTS (
                 SELECT 1
                 FROM unnest(p.device_names) dn
                 WHERE f.device_name ILIKE ('%'||dn||'%')
             ))
      )
),

/* ---------- detections filter (uses index) ---------- */
detections_filtered AS (
    SELECT
        d.frame_id,
        d.label,
        d.confidence,
        d.x1, d.y1, d.x2, d.y2
    FROM detections d, params p
    WHERE (p.endpoint_names IS NULL OR d.endpoint_name = ANY(p.endpoint_names))
      AND (p.labels IS NULL OR d.label = ANY(p.labels))
      AND d.confidence >= p.min_confidence
),

/* ---------- geometry creation and union (Expensive Step) ---------- */
/* Using MATERIALIZED here prevents the planner from re-running the Union logic
   when calculating medians vs final result */
frame_coverage AS MATERIALIZED (
    SELECT
        f.store_id,
        f.device_name,
        f.stream_id,
        f.timestamp,
        d.label,
        f.id AS frame_id,
        (ST_Area(ST_UnaryUnion(ST_Collect(
            ST_MakeEnvelope(LEAST(d.x1,d.x2), LEAST(d.y1,d.y2), GREATEST(d.x1,d.x2), GREATEST(d.y1,d.y2))
        ))) / f.frame_area) AS coverage,
        AVG(d.confidence) AS avg_confidence
    FROM frames_filtered f
    JOIN detections_filtered d ON d.frame_id = f.id
    GROUP BY f.store_id, f.device_name, f.stream_id, f.timestamp, d.label, f.id, f.frame_area
),

/* ---------- bucketing ---------- */
bucketed AS (
    SELECT
        fc.*,
        date_bin(p.bucket_interval, fc.timestamp, TIMESTAMP '1970-01-01') AS bucket_start,
        date_bin(p.bucket_interval, fc.timestamp, TIMESTAMP '1970-01-01') + p.bucket_interval AS bucket_end
    FROM frame_coverage fc, params p
),

/* ---------- Hampel filter (Median Deviation) ---------- */
bucket_medians AS (
    SELECT
        store_id,
        device_name,
        stream_id,
        label,
        bucket_start,
        bucket_end,
        percentile_disc(0.5) WITHIN GROUP (ORDER BY coverage) AS median_cov
    FROM bucketed
    GROUP BY store_id, device_name, stream_id, label, bucket_start, bucket_end
),

filtered AS (
    SELECT b.*
    FROM bucketed b
    JOIN bucket_medians m USING (store_id,device_name,stream_id,label,bucket_start,bucket_end)
    JOIN params p ON TRUE
    WHERE ABS(b.coverage - m.median_cov) <= p.max_deviation
),

annotated AS (
    SELECT
        f.*,
        COUNT(*) OVER (PARTITION BY store_id,device_name,stream_id,label,bucket_start) AS total_frames
    FROM filtered f
)

SELECT
    store_id,
    device_name,
    stream_id,
    label,
    bucket_start AS interval_start,
    bucket_end AS interval_end,
    ROUND(AVG(coverage)::numeric, 6) AS avg_coverage,
    ROUND(AVG(avg_confidence)::numeric, 6) AS avg_confidence,
    MAX(total_frames) AS total_frames
FROM annotated
GROUP BY store_id, device_name, stream_id, label, bucket_start, bucket_end
ORDER BY bucket_start, store_id, label;
"""
)


def _coerce_bucket_row(row: Any) -> FrameCoverageBucket:
    """
    Coerce a SQLAlchemy row mapping into the response model.
    """

    # avg_coverage comes back as Decimal due to ROUND(...::numeric, ...).
    avg_cov = row["avg_coverage"]
    avg_conf = row["avg_confidence"]
    return FrameCoverageBucket(
        store_id=row["store_id"],
        device_name=row["device_name"],
        stream_id=row["stream_id"],
        label=row["label"],
        interval_start=row["interval_start"],
        interval_end=row["interval_end"],
        avg_coverage=float(avg_cov) if avg_cov is not None else 0.0,
        avg_confidence=float(avg_conf) if avg_conf is not None else 0.0,
        total_frames=int(row["total_frames"]),
    )


async def _get_distinct_values_async(
    *, sql: str, params: dict[str, Any] | None = None
) -> list[str]:
    """
    Execute a distinct-values SQL query asynchronously and return strings.
    """

    async with AsyncSession(get_async_engine()) as db:
        result = await db.execute(text(sql), params or {})
        rows = result.fetchall()
        return [str(r[0]) for r in rows if r and r[0] is not None]


@router.get("/frames/coverage/values/store_id", response_model=list[str])
async def available_store_ids():
    """
    Return distinct store_id values for UI selection.
    """

    return await _get_distinct_values_async(
        sql="""
SELECT DISTINCT f.store_id::text AS value
FROM frames f
WHERE f.store_id IS NOT NULL
ORDER BY value ASC;
""",
    )


@router.get("/frames/coverage/values/stream_id", response_model=list[str])
async def available_stream_ids():
    """
    Return distinct stream_id values for UI selection.
    """

    return await _get_distinct_values_async(
        sql="""
SELECT DISTINCT f.stream_id::text AS value
FROM frames f
WHERE f.stream_id IS NOT NULL
ORDER BY value ASC;
""",
    )


@router.get("/frames/coverage/values/device_name", response_model=list[str])
async def available_device_names():
    """
    Return distinct device_name values for UI selection.
    """

    return await _get_distinct_values_async(
        sql="""
SELECT DISTINCT f.device_name AS value
FROM frames f
WHERE f.device_name IS NOT NULL
ORDER BY value ASC;
""",
    )


@router.get("/frames/coverage/values/label", response_model=list[str])
async def available_labels():
    """
    Return distinct detection labels for UI selection.
    """

    return await _get_distinct_values_async(
        sql="""
SELECT DISTINCT d.label AS value
FROM detections d
WHERE d.label IS NOT NULL
ORDER BY value ASC;
""",
    )


async def get_frame_coverage_buckets(
    request: Request,
    *,
    endpoint_name,
    label,
    min_confidence,
    bucket_interval,
    max_deviation,
    start_ts,
    end_ts,
    store_id,
    device_name,
):
    async with AsyncSession(get_async_engine()) as db:
        # get raw asyncpg connection
        conn = await db.connection()
        raw = await conn.get_raw_connection()
        driver = raw.driver_connection  # this is asyncpg.Connection

        async def run_query():
            await db.execute(text("SET LOCAL statement_timeout = '30s'"))
            await db.execute(text("SET LOCAL work_mem = '64MB'"))

            result = await db.execute(
                _FRAME_COVERAGE_SQL,
                {
                    "endpoint_name": endpoint_name,
                    "label": label,
                    "min_confidence": min_confidence,
                    "bucket_interval": bucket_interval,
                    "max_deviation": max_deviation,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "store_id": store_id,
                    "device_name": device_name,
                },
            )
            return result.mappings().all()

        async def watch_disconnect():
            while True:
                if await request.is_disconnected():
                    # THIS sends a Postgres CANCEL REQUEST
                    driver.terminate()
                    raise asyncio.CancelledError("Client disconnected")
                await asyncio.sleep(0.25)

        query_task = asyncio.create_task(run_query())
        cancel_task = asyncio.create_task(watch_disconnect())

        done, pending = await asyncio.wait(
            [query_task, cancel_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        rows = query_task.result()
        return [_coerce_bucket_row(r) for r in rows]


@router.get("/frames/coverage", response_model=list[FrameCoverageBucket])
async def frame_coverage(
    request: Request,
    endpoint_name: list[str] | None = Query(
        default=None,
        description="Detections endpoint_name filter. Repeat parameter to pass multiple values. Empty means include all.",
    ),
    label: list[str] | None = Query(
        default=None,
        description="Detection label filter. Repeat parameter to pass multiple values. Empty means include all.",
    ),
    min_confidence: float = Query(
        default=0.50,
        ge=0.0,
        le=1.0,
        description="Minimum confidence threshold",
    ),
    interval: str = Query(
        default="15 minutes",
        description="PostgreSQL interval string for bucketing (ex: '15 minutes')",
    ),
    store_id: list[str] | None = Query(
        default=None,
        description="Store id filter. Repeat parameter to pass multiple values. Empty means include all.",
    ),
    device_name: list[str] | None = Query(
        default=None,
        description="Device name substring filter. Repeat parameter to pass multiple values. Empty means include all.",
    ),
    start_ts: datetime | None = DateQuery(
        "start",
        description="Start timestamp (human readable, e.g. 'yesterday'). Default is no lower bound.",
    ),
    end_ts: datetime | None = DateQuery(
        "end",
        description="End timestamp (human readable, e.g. 'now'). Default is no upper bound.",
    ),
    max_deviation: float = Query(
        default=0.20,
        ge=0.0,
        le=1.0,
        description="Max allowed deviation from per-bucket median coverage (0.0 to 1.0)",
    ),
):
    """
    Execute the frame coverage summary query and return bucketed stats.

    The underlying SQL uses PostGIS functions (ST_MakeEnvelope, ST_Area) and
    PostgreSQL date_bin. Ensure the database has PostGIS enabled.
    """

    normalized_endpoint_names = _normalize_list(endpoint_name)
    normalized_store_ids = _normalize_list(store_id)
    normalized_labels = _normalize_list(label, lowercase=True)
    normalized_device_names = _normalize_list(device_name)

    return await get_frame_coverage_buckets(
        request=request,
        endpoint_name=normalized_endpoint_names,
        label=normalized_labels,
        min_confidence=min_confidence,
        bucket_interval=timedelta(seconds=pytimeparse.parse(interval)),
        max_deviation=max_deviation,
        start_ts=start_ts,
        end_ts=end_ts,
        store_id=normalized_store_ids,
        device_name=normalized_device_names,
    )


@router.post(
    "/frames/detections/bboxes",
    response_model=list[FrameDetectionsBBoxResponse],
)
async def frame_detection_bboxes(body: FrameDetectionsBBoxRequest):
    """
    Return detection bounding boxes for the given frame ids.

    Filters:
    - `label` (optional)
    - `min_confidence`
    """

    # Normalize: strip whitespace, drop empties, dedupe (keep order).
    seen: set[str] = set()
    frame_ids: list[str] = []
    for fid in body.frame_ids:
        fid = str(fid).strip()
        if not fid or fid in seen:
            continue
        seen.add(fid)
        frame_ids.append(fid)

    if not frame_ids:
        return []

    label = body.label.strip().lower() if body.label else None

    sql = text(
        """
WITH params AS NOT MATERIALIZED (
    SELECT
        CAST(:frame_ids AS text[]) AS frame_ids,
        CAST(:label AS text) AS label,
        CAST(:min_confidence AS float8) AS min_confidence
)
SELECT
    d.frame_id::text AS frame_id,
    d.label AS label,
    d.class_id AS class_id,
    d.confidence AS confidence,
    d.x1 AS x1,
    d.y1 AS y1,
    d.x2 AS x2,
    d.y2 AS y2
FROM detections d, params p
WHERE d.frame_id::text = ANY(p.frame_ids)
  AND (p.label IS NULL OR d.label = p.label)
  AND d.confidence >= p.min_confidence
ORDER BY d.frame_id::text ASC, d.confidence DESC;
"""
    )

    async with AsyncSession(get_async_engine()) as db:
        result = await db.execute(
            sql,
            {
                "frame_ids": frame_ids,
                "label": label,
                "min_confidence": float(body.min_confidence or 0.0),
            },
        )
        rows = result.mappings().all()

    grouped: dict[str, list[DetectionBBox]] = {fid: [] for fid in frame_ids}
    for row in rows:
        fid = str(row["frame_id"])
        grouped.setdefault(fid, []).append(
            DetectionBBox(
                label=row.get("label"),
                class_id=row.get("class_id"),
                confidence=float(row["confidence"]),
                x1=float(row["x1"]),
                y1=float(row["y1"]),
                x2=float(row["x2"]),
                y2=float(row["y2"]),
            )
        )

    return [
        FrameDetectionsBBoxResponse(frame_id=fid, boxes=grouped.get(fid, []))
        for fid in frame_ids
    ]
