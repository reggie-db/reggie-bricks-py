import asyncio
import base64
import os
from pathlib import Path

from dbx_core import paths
from dbx_tools import clients
from fastapi import HTTPException
from lfp_logging import logs
from pyspark.sql import functions as F

LOG = logs.logger()
# ---- config ----
BATCH_WINDOW_SEC = 0.1  # 100ms
CACHE_DIR = Path(paths.temp_dir() / __name__ / "disk_cache_v2")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CONTENT_TABLE = os.environ.get("CONTENT_TABLE", "reggie_pierce.iot_ingest.raw_v2")


# ---- disk cache ----
def cache_path(image_id: str) -> Path:
    return CACHE_DIR / f"{image_id}.bin"


def read_cache(image_id: str) -> bytes | None:
    path = cache_path(image_id)
    if path.exists():
        return path.read_bytes()
    return None


def write_cache(image_id: str, data: bytes) -> None:
    cache_path(image_id).write_bytes(data)


# ---- batching state ----
_pending: dict[str, list[asyncio.Future]] = {}
_batch_lock = asyncio.Lock()
_batch_task: asyncio.Task | None = None


# ---- placeholder spark query ----
async def spark_fetch(ids: list[str]) -> dict[str, bytes]:
    """
    Placeholder for Spark query.
    Expected behavior later:
      SELECT id, content FROM table WHERE id IN (...)
    Return: {id: content_binary}
    """
    # simulate latency
    result = {}
    spark = clients.spark()
    rows = (
        spark.read.table(CONTENT_TABLE)
        .filter(F.col("id").isin(ids))
        .select("id", F.base64(F.col("content")).alias("content_b64"))
        .collect()
    )
    for row in rows:
        id = row.id
        content_b64 = row.content_b64
        result[id] = base64.b64decode(content_b64)
    for id in ids:
        if id not in result:
            result[id] = None
    LOG.debug(f"Fetched {result}")
    return result


async def _run_batch():
    global _batch_task
    await asyncio.sleep(BATCH_WINDOW_SEC)

    async with _batch_lock:
        batch = dict(_pending)
        _pending.clear()
        _batch_task = None

    ids = list(batch.keys())
    if not ids:
        return

    try:
        results = await spark_fetch(ids)
    except Exception as e:
        for futures in batch.values():
            for fut in futures:
                if not fut.done():
                    fut.set_exception(e)
        return

    for image_id, futures in batch.items():
        data = results.get(image_id)
        if data is None:
            for fut in futures:
                if not fut.done():
                    fut.set_exception(HTTPException(status_code=404))
            continue

        write_cache(image_id, data)
        for fut in futures:
            if not fut.done():
                fut.set_result(data)


async def enqueue(image_id: str) -> bytes:
    global _batch_task

    fut = asyncio.get_running_loop().create_future()

    async with _batch_lock:
        _pending.setdefault(image_id, []).append(fut)
        if _batch_task is None:
            _batch_task = asyncio.create_task(_run_batch())

    return await fut


# ---- API ----
