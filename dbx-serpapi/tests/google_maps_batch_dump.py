"""Batch Google Maps lookups from a store JSON file.

This script is intentionally a plain executable script, not a pytest test.
It reads a JSON list of store objects and writes one SerpAPI response per store
to ``./tmp/{SID}.json``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
from pathlib import Path
from typing import Any

from dbx_core import projects
from lfp_logging import logs
from serpapi.exceptions import HTTPError as SerpHttpError

from dbx_serpapi.serpapis import search_google_maps

LOG = logs.logger()


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments for input path and API key."""
    parser = argparse.ArgumentParser(
        description=(
            "Read store records from JSON and dump one Google Maps SerpAPI "
            "response per SID into ./tmp."
        )
    )
    parser.add_argument(
        "input_file",
        help="Path to JSON file containing a list of store records.",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("SERPAPI_API_KEY"),
        help="SerpAPI key. Defaults to SERPAPI_API_KEY when omitted.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of store records to process.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker threads for concurrent API calls.",
    )
    return parser.parse_args()


def _require_value(record: dict[str, Any], key: str) -> Any:
    """Return a required field from a store record."""
    if key not in record:
        raise ValueError(f"Missing required field '{key}' in record: {record}")
    return record[key]


def _query_from_record(record: dict[str, Any]) -> tuple[str, float, float, int]:
    """Build query inputs from one store record."""
    sid = int(_require_value(record, "SID"))
    brand = str(_require_value(record, "brand")).strip()
    lat = float(_require_value(record, "LAT"))
    lon = float(_require_value(record, "LON"))
    return brand, lat, lon, sid


def _write_record_result(record: dict[str, Any], out_dir: Path) -> str:
    """Fetch SerpAPI results for one record and write output JSON."""
    query, lat, lon, sid = _query_from_record(record)
    output_path = out_dir / f"{sid}.json"
    if output_path.exists():
        return f"skipped:{sid}:{output_path}"
    results = search_google_maps(
        q=query,
        lat=lat,
        long=lon,
    )
    output_path.write_text(
        json.dumps(results.as_dict(), indent=2),
        encoding="utf-8",
    )
    return f"wrote:{sid}:{output_path}"


def _log_response_error(error: Exception, sid: int | str) -> None:
    """Log SerpAPI/requests response payload details when available."""
    # SerpAPI wraps requests errors; walk the exception chain to find a response body.
    exc: BaseException | None = error
    while exc is not None:
        response = getattr(exc, "response", None)
        if response is not None:
            status_code = getattr(response, "status_code", None)
            reason = getattr(response, "reason", None)
            text = getattr(response, "text", None)
            LOG.error(
                "SID %s request failed: status=%s reason=%s", sid, status_code, reason
            )
            if text:
                LOG.error("SID %s response body: %s", sid, text)
            return
        exc = exc.__cause__
    LOG.error("SID %s request failed without response payload: %s", sid, error)


def main() -> int:
    """Execute batch SerpAPI lookups and write output files."""
    args = _parse_args()
    if not args.api_key:
        raise ValueError("SERPAPI api key required (use --api-key or SERPAPI_API_KEY)")

    input_path = Path(args.input_file)
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Input JSON must be a list of store objects")
    if args.limit is not None and args.limit < 1:
        raise ValueError("--limit must be >= 1")
    if args.workers < 1:
        raise ValueError("--workers must be >= 1")

    out_dir = projects.root_dir() / ".tmp"
    out_dir.mkdir(parents=True, exist_ok=True)

    records = payload[: args.limit] if args.limit is not None else payload
    for record in records:
        if not isinstance(record, dict):
            raise ValueError(f"Each list item must be an object, got: {type(record)}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_record = {
            executor.submit(_write_record_result, record, out_dir): record
            for record in records
        }
        for future in concurrent.futures.as_completed(future_to_record):
            record = future_to_record[future]
            sid = record.get("SID", "unknown")
            try:
                status = future.result()
            except SerpHttpError as exc:
                _log_response_error(exc, sid)
                executor.shutdown(wait=False, cancel_futures=True)
                raise
            except Exception:
                executor.shutdown(wait=False, cancel_futures=True)
                raise

            action, sid, output_path = status.split(":", 2)
            if action == "skipped":
                LOG.info(
                    "Skipping SID %s, output already exists at %s", sid, output_path
                )
            else:
                LOG.info("Wrote %s", output_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
