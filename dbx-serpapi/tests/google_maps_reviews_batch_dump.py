"""Batch Google Maps review lookups from stored location search responses.

This script is intentionally a plain executable script, not a pytest test.
It reads ``./.tmp/racetrac-locations/{SID}.json`` files, extracts a place id,
calls SerpAPI google maps reviews, and writes one output per SID to
``./.tmp/racetrac-location-reviews/{SID}.json``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
from pathlib import Path
from typing import Any

from dbx_core import projects
from lfp_logging import logs

from dbx_serpapi.serpapis import google_maps_reviews

LOG = logs.logger()


def _parse_args() -> argparse.Namespace:
    """Parse command-line options for input/output paths and limits."""
    root_dir = projects.root_dir()
    parser = argparse.ArgumentParser(
        description=(
            "Read .tmp/racetrac-locations/*.json, fetch google_maps_reviews "
            "for each place_id, and write output files."
        )
    )
    parser.add_argument(
        "--input-dir",
        default=str(root_dir / ".tmp" / "racetrac-locations"),
        help="Directory containing SID.json Google Maps search results.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(root_dir / ".tmp" / "racetrac-location-reviews"),
        help="Directory to write SID.json Google Maps review results.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of SID files to process.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker threads for concurrent review requests.",
    )
    return parser.parse_args()


def _extract_place_id(payload: dict[str, Any], sid: str) -> str:
    """Extract place_id from a Google Maps search payload."""
    local_results = payload.get("local_results", None)
    if isinstance(local_results, list):
        for item in local_results:
            if isinstance(item, dict):
                place_id = item.get("place_id", None)
                if isinstance(place_id, str) and place_id:
                    return place_id
    raise ValueError(f"No place_id found in local_results for SID {sid}")


def _process_sid_file(input_file: Path, output_dir: Path) -> str:
    """Process one SID search-result file and write the reviews response."""
    sid = input_file.stem
    output_file = output_dir / f"{sid}.json"
    if output_file.exists():
        return f"skipped:{sid}:{output_file}"

    payload = json.loads(input_file.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected object payload in {input_file}")

    try:
        place_id = _extract_place_id(payload, sid)
    except ValueError:
        output_file.write_text(
            json.dumps({}, indent=2),
            encoding="utf-8",
        )
        return f"empty:{sid}:{output_file}"

    results = google_maps_reviews(place_id=place_id)
    output_file.write_text(
        json.dumps(results.as_dict(), indent=2),
        encoding="utf-8",
    )
    return f"wrote:{sid}:{output_file}"


def main() -> int:
    """Run review fetches for each SID file in the input directory."""
    args = _parse_args()
    if args.limit is not None and args.limit < 1:
        raise ValueError("--limit must be >= 1")
    if args.workers < 1:
        raise ValueError("--workers must be >= 1")

    input_dir = Path(args.input_dir)
    if not input_dir.is_dir():
        raise ValueError(f"Input directory not found: {input_dir}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    input_files = sorted(input_dir.glob("*.json"))
    if args.limit is not None:
        input_files = input_files[: args.limit]

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_file = {
            executor.submit(_process_sid_file, input_file, output_dir): input_file
            for input_file in input_files
        }
        for future in concurrent.futures.as_completed(future_to_file):
            try:
                status = future.result()
            except Exception:
                executor.shutdown(wait=False, cancel_futures=True)
                raise
            action, sid, output_path = status.split(":", 2)
            if action == "skipped":
                LOG.info(
                    "Skipping SID %s, output already exists at %s", sid, output_path
                )
            elif action == "empty":
                LOG.info("Wrote empty JSON for SID %s to %s", sid, output_path)
            else:
                LOG.info("Wrote SID %s reviews to %s", sid, output_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
