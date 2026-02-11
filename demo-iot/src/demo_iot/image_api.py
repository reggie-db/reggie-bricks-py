import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dbx_core import paths
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from lfp_logging import logs
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from demo_iot import image_service
from demo_iot.database import get_async_engine
from demo_iot.date_utils import DateQuery

LOG = logs.logger()

# ---- config ----
CACHE_DIR = Path(paths.temp_dir() / __name__ / "query_cache_v2")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(key: str) -> Path:
    return CACHE_DIR / f"{key}.json"


def _read_cache(key: str) -> list[str] | None:
    path = _cache_path(key)
    if path.exists():
        try:
            data = json.loads(path.read_text())
            expires_at = datetime.fromisoformat(data["expires_at"])
            if expires_at > datetime.now(timezone.utc):
                return data["value"]
            else:
                path.unlink()  # Expired
        except Exception as e:
            LOG.error(f"Error reading cache {key}: {e}")
    return None


def _write_cache(key: str, value: list[str], ttl: int) -> None:
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl)
    data = {
        "value": value,
        "expires_at": expires_at.isoformat(),
    }
    _cache_path(key).write_text(json.dumps(data))


router = APIRouter()


@router.get("/image/{image_id}")
async def get_image(image_id: str):
    """
    Return a cached image by id.

    If the image is not present in the local disk cache, it is fetched via the
    image batching queue (`image_service.enqueue`) and then cached.
    """

    cached = image_service.read_cache(image_id)
    if cached is not None:
        LOG.debug(f"Serving image {image_id} from cache")
        return Response(content=cached, media_type="image/jpeg")

    try:
        LOG.debug(f"Enqueuing image request for {image_id}")
        data = await image_service.enqueue(image_id)
        LOG.debug(f"Successfully retrieved image {image_id} (size: {len(data)} bytes)")
    except HTTPException:
        raise
    except Exception as e:
        LOG.error(f"Error retrieving image {image_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return Response(content=data, media_type="image/jpeg")


@router.get("/frames/ids", response_model=list[str])
async def list_frame_ids(
    stream_id: str = Query(description="Stream id to filter frames by"),
    start_ts: datetime | None = DateQuery(
        "start", description="Start timestamp (human readable, e.g. 'yesterday')"
    ),
    end_ts: datetime | None = DateQuery(
        "end", description="End timestamp (human readable, e.g. 'now')"
    ),
    label: str | None = Query(
        default=None,
        description="Optional detection label. When provided, only frames that have at least one detection with this label are returned.",
    ),
    min_confidence: float | None = Query(
        default=None,
        ge=0.0,
        le=1.0,
        description="Optional minimum detection confidence (0.0 to 1.0). When provided, only frames that have at least one detection with confidence >= this value are returned.",
    ),
    limit: int = Query(
        default=500,
        ge=1,
        description="Maximum number of frame ids to return",
    ),
    ttl: int = Query(
        default=3600,
        ge=0,
        description="Cache TTL in seconds (0 to disable)",
    ),
):
    """
    Return a list of frame ids for a stream within a time range.

    The results are ordered by frame timestamp, then id, and limited by `limit`.
    """

    # Normalize and create cache key
    normalized_label = label.strip().lower() if label else None
    normalized_min_confidence = (
        float(min_confidence) if min_confidence is not None else None
    )
    params = {
        "stream_id": stream_id,
        "start_ts": start_ts.isoformat() if start_ts else None,
        "end_ts": end_ts.isoformat() if end_ts else None,
        "label": normalized_label,
        "min_confidence": normalized_min_confidence,
        "limit": limit,
    }
    key_str = json.dumps(params, sort_keys=True, default=str)
    cache_key = hashlib.sha256(key_str.encode()).hexdigest()

    # Check cache
    if ttl > 0:
        cached_value = _read_cache(cache_key)
        if cached_value is not None:
            LOG.debug(f"Cache hit for {cache_key}")
            return cached_value

    async with AsyncSession(get_async_engine()) as db:
        sql = text(
            """
WITH frames_filtered AS (
    SELECT
        f.id,
        f.timestamp
    FROM frames f
    WHERE f.stream_id = :stream_id
      AND f.timestamp IS NOT NULL
      AND (CAST(:start_ts AS timestamptz) IS NULL OR f.timestamp >= CAST(:start_ts AS timestamptz))
      AND (CAST(:end_ts AS timestamptz) IS NULL OR f.timestamp <= CAST(:end_ts AS timestamptz))
      AND (
            (CAST(:label AS text) IS NULL AND CAST(:min_confidence AS float8) IS NULL)
         OR EXISTS (
                SELECT 1
                FROM detections d
                WHERE d.frame_id = f.id
                  AND (CAST(:label AS text) IS NULL OR d.label = CAST(:label AS text))
                  AND (CAST(:min_confidence AS float8) IS NULL OR d.confidence >= CAST(:min_confidence AS float8))
            )
      )
),
ranked_frames AS (
    SELECT 
        ff.id, 
        ff.timestamp,
        row_number() OVER (ORDER BY ff.timestamp ASC, ff.id ASC) - 1 as idx,
        count(*) OVER () as total
    FROM frames_filtered ff
)
SELECT id::text
FROM ranked_frames
WHERE total <= :limit 
   OR idx % (GREATEST(total / :limit, 1)) = 0
ORDER BY timestamp ASC, id ASC
LIMIT :limit;
"""
        )

        result = await db.execute(
            sql,
            {
                "stream_id": stream_id,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "label": normalized_label,
                "min_confidence": normalized_min_confidence,
                "limit": limit,
            },
        )

        rows = result.fetchall()
        frame_ids = [r[0] for r in rows if r and r[0] is not None]

        # Write to cache
        if ttl > 0:
            _write_cache(cache_key, frame_ids, ttl)
            LOG.debug(f"Cache miss for {cache_key}, stored to disk with TTL {ttl}s")

        return frame_ids
