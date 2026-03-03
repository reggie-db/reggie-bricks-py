from __future__ import annotations

import json
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address, ip_address
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import urlopen

from dbx_core.parsers import to_bool, to_float

"""
IP information service helpers.

This module provides a small dataclass-backed API for looking up IP information
from `ipwho.is`:

- `info()` for the current caller IP
- `info("8.8.8.8")` for a specific IP address or hostname
- `info(IPv6Address("2001:4860:4860::8888"))` for typed IP objects
"""

_IPWHO_URL = "https://ipwho.is"
_TIMEOUT_SECONDS = 10.0
_Address = str | IPv4Address | IPv6Address | None


@dataclass(slots=True)
class IpInfo:
    """Parsed IP metadata from ipwho.is."""

    ip: str | None = None
    success: bool | None = None
    type: str | None = None
    continent: str | None = None
    continent_code: str | None = None
    country: str | None = None
    country_code: str | None = None
    region: str | None = None
    region_code: str | None = None
    city: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    is_eu: bool | None = None
    postal: str | None = None
    calling_code: str | None = None
    capital: str | None = None
    borders: str | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> IpInfo:
        """Create an ``IpInfo`` from a JSON payload."""
        return cls(
            ip=_as_str(payload.get("ip")),
            success=to_bool(payload.get("success"), default=None),
            type=_as_str(payload.get("type")),
            continent=_as_str(payload.get("continent")),
            continent_code=_as_str(payload.get("continent_code")),
            country=_as_str(payload.get("country")),
            country_code=_as_str(payload.get("country_code")),
            region=_as_str(payload.get("region")),
            region_code=_as_str(payload.get("region_code")),
            city=_as_str(payload.get("city")),
            latitude=to_float(payload.get("latitude")),
            longitude=to_float(payload.get("longitude")),
            is_eu=to_bool(payload.get("is_eu"), default=None),
            postal=_as_str(payload.get("postal")),
            calling_code=_as_str(payload.get("calling_code")),
            capital=_as_str(payload.get("capital")),
            borders=_as_str(payload.get("borders")),
            raw=payload,
        )


def info(address: _Address = None) -> IpInfo:
    """
    Fetch IP metadata for an optional address.

    Args:
        address: Optional IP address value (string or `ipaddress` object) or
            hostname. When omitted, ipwho.is resolves the caller's public IP.

    Returns:
        Parsed ``IpInfo``.
    """
    payload = _fetch_payload(address=address)
    return IpInfo.from_payload(payload)


def _fetch_payload(address: _Address) -> dict[str, Any]:
    """Call ipwho.is and return a parsed JSON payload."""
    url = _build_url(address)
    try:
        with urlopen(url, timeout=_TIMEOUT_SECONDS) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        raise RuntimeError(f"ipwho.is request failed with HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"ipwho.is request failed: {exc.reason}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("ipwho.is returned invalid JSON") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("ipwho.is returned a non-object payload")
    return payload


def _build_url(address: _Address) -> str:
    """Build the ipwho.is URL with an optional target address."""
    if address is None:
        return f"{_IPWHO_URL}/"
    if not (clean_address := _normalize_address(address)):
        return f"{_IPWHO_URL}/"
    # Quote the input so hostnames and IPv6 strings remain path-safe.
    return f"{_IPWHO_URL}/{quote(clean_address, safe='')}"


def _as_str(value: Any) -> str | None:
    if value:
        if value_str := str(value).strip():
            return value_str
    return None


def _normalize_address(address: _Address) -> str | None:
    """Normalize a target address for consistent API lookup."""
    if address is None:
        return None
    if isinstance(address, (IPv4Address, IPv6Address)):
        return address.compressed
    if not (candidate := str(address).strip()):
        return None
    try:
        # Normalize valid IPv4/IPv6 inputs while still allowing hostnames.
        return ip_address(candidate).compressed
    except ValueError:
        return candidate
