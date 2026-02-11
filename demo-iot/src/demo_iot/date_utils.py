from datetime import datetime
from typing import Annotated, Any

import dateparser
from fastapi import Depends, HTTPException, Query


def parse_datetime(value: str | datetime | None) -> datetime | None:
    """
    Parse a date/time string using dateparser for human-readable formats.
    If the value is already a datetime or None, it is returned as-is.
    """
    if value is None or isinstance(value, datetime):
        return value

    parsed = dateparser.parse(str(value))
    if parsed is None:
        raise HTTPException(
            status_code=400, detail=f"Could not parse date/time string: '{value}'"
        )
    return parsed


def DateQuery(name: str, description: str | None = None, default: Any = None):
    """
    A FastAPI dependency that parses a human-readable date/time query parameter.
    """

    def _parse(value: str | None = Query(default, alias=name, description=description)):
        return parse_datetime(value)

    return Depends(_parse)


# Annotated type for use in FastAPI dependencies/annotations
HumanDateTime = Annotated[
    datetime | None,
    Query(
        description="Date/time string (human readable, e.g. 'yesterday', '2026-01-01')"
    ),
]
