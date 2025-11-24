# franchise_fillers.py
# Generic franchise filler with minimal brand specifics.
# Uses a GENERATORS map from short name -> fill function, creates FranchiseInfo per entry,
# fills the matching HTML template found recursively under TEMPLATE_ROOT, and appends a summary.

import argparse
import io
import os
import random
import threading
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime as dt
from typing import Any

from databricks.sdk.core import Config
from reggie_core import funcs, logs, objects, strs
from reggie_tools import clients, configs
from xhtml2pdf import pisa

from generators_franchise import franchise_agreement
from generators_franchise.franchise_agreement import FranchiseAgreementInfo

LOG = logs.logger(__file__)


FRANCHISE_BRANDS = [
    "McDonald’s",
    "Subway",
    "Starbucks",
    "Chick-fil-A",
    "Taco Bell",
    "Wendy’s",
    "Burger King",
    "Domino’s Pizza",
    "Dunkin’",
    "KFC",
    "Pizza Hut",
    "Ace Hardware",
    "The UPS Store",
    "Popeyes Louisiana Kitchen",
    "Little Caesars",
    "Jersey Mike’s Subs",
    "Keller Williams Realty",
    "Anytime Fitness",
    "RE/MAX",
    "Great Clips",
    "H&R Block",
    "Liberty Tax Service",
    "Cruise Planners",
    "Smoothie King",
    "Jimmy John’s",
    "Panera Bread",
    "Culver’s",
    "Kumon",
    "Baskin-Robbins",
    "Dairy Queen",
    "Carl’s Jr.",
    "Sonic Drive-In",
    "Arby’s",
    "Papa John’s",
    "Firehouse Subs",
    "Buffalo Wild Wings",
    "Checkers & Rally’s",
    "7-Eleven",
    "A&W Restaurants",
    "IHOP",
    "Whataburger",
    "Wingstop",
    "Hampton by Hilton",
    "Subway",
    "Dollar Tree",
    "Sport Clips",
    "Great Clips",
    "Massage Envy",
]

# -----------------------
# Orchestration
# -----------------------


_render_thread_local = threading.local()


def _html_to_pdf(html_str: str) -> io.BytesIO:
    pdf_bytes = io.BytesIO()
    status = pisa.CreatePDF(src=html_str, dest=pdf_bytes, encoding="utf-8")
    if status.err:
        # inspect status.log for details, or raise
        raise ValueError(f"xhtml2pdf failed to render HTML to PDF {status}")
    pdf_bytes.seek(0)  # rewind so the next reader starts at byte 0
    return pdf_bytes


def _to_json_bytes(data: Any) -> io.BytesIO:
    json_str = objects.to_json(data)
    buf = io.BytesIO(json_str.encode("utf-8"))
    buf.seek(0)
    return buf


def _render(
    config_dict: dict,
    volume_path: str,
    franchise_brand: str,
) -> str:
    if not hasattr(_render_thread_local, "workspace_client"):
        config = Config(**config_dict)
        _render_thread_local.workspace_client = clients.workspace_client(config)

    workspace_client = _render_thread_local.workspace_client
    info = FranchiseAgreementInfo(brand_name=franchise_brand)
    html = franchise_agreement.generate(info)

    file_name = f"{'_'.join(strs.tokenize(franchise_brand))}_{int(time.time() * 1000)}_{uuid.uuid4().hex}"

    pdf_bytes = _html_to_pdf(html)
    pdf_dir_path = f"pdfs/{file_name}.pdf"
    pdf_volume_path = f"{volume_path}/{pdf_dir_path}"
    workspace_client.files.upload(pdf_volume_path, contents=pdf_bytes)
    metadata = {
        "file_path": pdf_dir_path,
        "created_at": dt.now().isoformat(),
        "info": funcs.dump(info, recursive=True),
    }
    json_bytes = _to_json_bytes(metadata)
    json_volume_path = f"{volume_path}/metadata/{file_name}.json"
    workspace_client.files.upload(json_volume_path, contents=json_bytes)
    return file_name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile",
        type=str,
        default="FIELD-ENG-EAST",
        help="Databricks config profile",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="reggie_pierce",
        help="Catalog name",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default="franchise",
        help="Schema name",
    )

    parser.add_argument(
        "--volume",
        type=str,
        default="franchise_agreements",
        help="Volume name",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of runs to generate per selected franchise",
    )
    parser.add_argument(
        "--franchise",
        nargs="+",
        help="One or more franchises to generate; omit to run all",
    )
    args = parser.parse_args()
    config = configs.get(args.profile)
    if args.franchise:
        franchise_brands = args.franchise
    else:
        franchise_brands = FRANCHISE_BRANDS.copy()

    random.shuffle(franchise_brands)

    volume_path = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}"

    jobs = []
    while len(jobs) < args.runs:
        for n in franchise_brands:
            jobs.append(n)
            if len(jobs) >= args.runs:
                break
    total = len(jobs)
    counter = 0
    max_workers = os.cpu_count() or 1
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(
                _render,
                config.to_dict(),
                volume_path,
                n,
            )
            for n in jobs
        ]
        for fut in as_completed(futures):
            counter += 1
            LOG.info(f"Uploaded pdf {counter} of {total} - file_name: {fut.result()}")

    LOG.info(f"Total: {total}")


if __name__ == "__main__":
    main()
