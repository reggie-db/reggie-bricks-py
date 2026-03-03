# pyright: reportMissingImports=false

import json
from ipaddress import IPv6Address

from dbx_ip import ips


class _FakeHttpResponse:
    def __init__(self, payload: dict):
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_info_uses_default_address_when_not_provided(monkeypatch):
    calls: list[str] = []

    def _fake_urlopen(url: str, timeout: float):
        calls.append(url)
        assert timeout == 10.0
        return _FakeHttpResponse(
            {
                "success": True,
                "ip": "203.0.113.42",
                "latitude": 1.23,
                "longitude": 4.56,
            }
        )

    monkeypatch.setattr(ips, "urlopen", _fake_urlopen)

    data = ips.info()
    assert data.success is True
    assert data.ip == "203.0.113.42"
    assert data.latitude == 1.23
    assert data.longitude == 4.56
    assert calls == ["https://ipwho.is/"]


def test_info_uses_well_known_ip_when_provided(monkeypatch):
    calls: list[str] = []

    def _fake_urlopen(url: str, timeout: float):
        calls.append(url)
        assert timeout == 10.0
        return _FakeHttpResponse(
            {
                "success": True,
                "ip": "8.8.8.8",
                "country": "United States",
                "latitude": "37.386",
                "longitude": "-122.0838",
            }
        )

    monkeypatch.setattr(ips, "urlopen", _fake_urlopen)

    data = ips.info("8.8.8.8")
    assert data.success is True
    assert data.ip == "8.8.8.8"
    assert data.country == "United States"
    assert data.latitude == 37.386
    assert data.longitude == -122.0838
    assert calls == ["https://ipwho.is/8.8.8.8"]


def test_info_handles_ipv6_address(monkeypatch):
    calls: list[str] = []

    def _fake_urlopen(url: str, timeout: float):
        calls.append(url)
        assert timeout == 10.0
        return _FakeHttpResponse(
            {
                "success": True,
                "ip": "2001:4860:4860::8888",
                "type": "IPv6",
            }
        )

    monkeypatch.setattr(ips, "urlopen", _fake_urlopen)

    data = ips.info("2001:4860:4860::8888")
    assert data.success is True
    assert data.type == "IPv6"
    assert calls == ["https://ipwho.is/2001%3A4860%3A4860%3A%3A8888"]


def test_info_handles_typed_ipv6_address(monkeypatch):
    calls: list[str] = []

    def _fake_urlopen(url: str, timeout: float):
        calls.append(url)
        assert timeout == 10.0
        return _FakeHttpResponse({"success": True, "ip": "2001:4860:4860::8888"})

    monkeypatch.setattr(ips, "urlopen", _fake_urlopen)

    data = ips.info(IPv6Address("2001:4860:4860:0:0:0:0:8888"))
    assert data.success is True
    assert data.ip == "2001:4860:4860::8888"
    assert calls == ["https://ipwho.is/2001%3A4860%3A4860%3A%3A8888"]
