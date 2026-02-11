#!/usr/bin/env python3

import argparse
import json
import sys
import urllib.error
import urllib.request

from databricks.sdk.config import Config
from lfp_logging import logs

from dbx_tools import configs

LOG = logs.logger()


def _reset_checkpoint(config: Config, pipeline_id: str, flow: list[str]):
    url = f"{config.host.rstrip('/')}/api/2.0/pipelines/{pipeline_id}/updates"

    payload = {"reset_checkpoint_selection": flow}

    req = urllib.request.Request(
        url=url,
        method="POST",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {configs.token(config)}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode()
            LOG.info("SUCCESS")
            LOG.info(body)
    except urllib.error.HTTPError as e:
        LOG.error(f"HTTP {e.code}")
        LOG.error(e.read().decode())
        sys.exit(1)
    except urllib.error.URLError as e:
        LOG.error(f"Connection error: {e}")
        sys.exit(2)


def main():
    parser = argparse.ArgumentParser(
        description="Reset DLT streaming checkpoint selection for specific flows"
    )

    parser.add_argument(
        "--pipeline",
        required=True,
        help="DLT pipeline ID",
    )

    parser.add_argument(
        "--flow",
        required=True,
        nargs="+",
        help="One or more streaming flow names to reset",
    )

    args = parser.parse_args()

    LOG.info(f"Pipeline: {args.pipeline}")
    LOG.info(f"Flows: {', '.join(args.flow)}")
    config = configs.get()

    _reset_checkpoint(config, args.pipeline, args.flow)


if __name__ == "__main__":
    main()
