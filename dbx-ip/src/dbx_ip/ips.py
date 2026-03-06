from __future__ import annotations

import json
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address, ip_address
from typing import Any, TypeAlias
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import urlopen

from dbx_core.parsers import to_bool, to_float
from dbx_core.strs import trim

"""
IP information service helpers.

This module provides a small dataclass-backed API for looking up IP information
from `ipwho.is`:

- `info()` for the current caller IP
- `info("8.8.8.8")` for a specific IP address or hostname
- `info(IPv6Address("2001:4860:4860::8888"))` for typed IP objects
"""

IPAddressLike: TypeAlias = str | IPv4Address | IPv6Address | None
_IPWHO_URL = "https://ipwho.is"
_TIMEOUT_SECONDS_DEFAULT = 10.0


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
            ip=trim(payload.get("ip"), default=None),
            success=to_bool(payload.get("success"), default=None),
            type=trim(payload.get("type"), default=None),
            continent=trim(payload.get("continent"), default=None),
            continent_code=trim(payload.get("continent_code"), default=None),
            country=trim(payload.get("country"), default=None),
            country_code=trim(payload.get("country_code"), default=None),
            region=trim(payload.get("region"), default=None),
            region_code=trim(payload.get("region_code"), default=None),
            city=trim(payload.get("city"), default=None),
            latitude=to_float(payload.get("latitude")),
            longitude=to_float(payload.get("longitude")),
            is_eu=to_bool(payload.get("is_eu"), default=None),
            postal=trim(payload.get("postal"), default=None),
            calling_code=trim(payload.get("calling_code"), default=None),
            capital=trim(payload.get("capital"), default=None),
            borders=trim(payload.get("borders"), default=None),
            raw=payload,
        )


def info(address: IPAddressLike = None, **kwargs: Any) -> IpInfo:
    """
    Fetch IP metadata for an optional address.

    Args:
        address: Optional IP address value (string or `ipaddress` object) or
            hostname. When omitted, ipwho.is resolves the caller's public IP.

    Returns:
        Parsed ``IpInfo``.
    """
    url = _build_url(address)
    if not "timeout" in kwargs:
        kwargs["timeout"] = _TIMEOUT_SECONDS_DEFAULT
    try:
        with urlopen(url, **kwargs) as response:
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
    return IpInfo.from_payload(payload)


def _build_url(address: IPAddressLike) -> str:
    """Build the ipwho.is URL with an optional target address."""
    url = _IPWHO_URL
    if address:
        if address_str := _address_str(address):
            url += f"/{quote(address_str, safe='')}"
    return url


def _address_str(address: IPAddressLike) -> str | None:
    """Normalize a target address for consistent API lookup."""
    if isinstance(address, (IPv4Address, IPv6Address)):
        return address.compressed
    elif candidate := trim(address, default=None):
        try:
            # Normalize valid IPv4/IPv6 inputs while still allowing hostnames.
            return ip_address(candidate).compressed
        except ValueError:
            pass
    return candidate


if __name__ == "__main__":
    from dbx_core import objects

    print(json.dumps(objects.dump(info()), indent=2))
