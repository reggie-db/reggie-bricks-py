import io
import json
import pathlib
import subprocess
import tarfile
import zipfile


def _tar_gz_bytes(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o755
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _zip_bytes(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


class _FakeHTTPResponse:
    def __init__(self, data: bytes):
        self._bio = io.BytesIO(data)

    def read(self, n: int = -1) -> bytes:
        return self._bio.read(n)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_path_prefers_system_binary(monkeypatch):
    from dbx_caddy import caddy

    monkeypatch.setattr(caddy.shutil, "which", lambda _: "/usr/bin/caddy")
    monkeypatch.setattr(
        caddy,
        "install",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("install() should not be called")
        ),
    )

    assert caddy.path() == pathlib.Path("/usr/bin/caddy")


def test_path_installs_when_missing(monkeypatch, tmp_path):
    from dbx_caddy import caddy

    monkeypatch.setattr(caddy.shutil, "which", lambda _: None)
    expected = tmp_path / "bin" / "caddy"
    monkeypatch.setattr(caddy, "_install_path", lambda: expected)

    calls: list[pathlib.Path] = []

    def _install(dst=None):
        calls.append(dst)
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(b"caddy")
        return dst

    monkeypatch.setattr(caddy, "install", _install)

    p = caddy.path(system_binary=False)
    assert p == expected
    assert expected.exists()
    assert calls == [expected]


def test_install_downloads_and_extracts_latest_release_tar_gz(monkeypatch, tmp_path):
    from dbx_caddy import caddy

    monkeypatch.setenv("HOME", str(tmp_path))

    # Force deterministic platform mapping.
    monkeypatch.setattr(caddy.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(caddy.platform, "machine", lambda: "arm64")

    asset_name = "caddy_2.10.2_mac_arm64.tar.gz"
    asset_url = "https://example.invalid/caddy.tar.gz"
    release = {
        "tag_name": "v2.10.2",
        "assets": [
            {
                "name": "caddy_2.10.2_src.tar.gz",
                "browser_download_url": "https://example.invalid/src.tgz",
            },
            {"name": asset_name, "browser_download_url": asset_url},
        ],
    }

    caddy_bytes = b"#!/bin/sh\necho caddy\n"
    archive_bytes = _tar_gz_bytes({"caddy": caddy_bytes})

    def _urlopen(req, timeout=30):
        url = getattr(req, "full_url", req)
        if url == caddy._LATEST_RELEASE_URL:
            return _FakeHTTPResponse(json.dumps(release).encode("utf-8"))
        if url == asset_url:
            return _FakeHTTPResponse(archive_bytes)
        raise AssertionError(f"Unexpected urlopen URL: {url!r}")

    monkeypatch.setattr(caddy.urllib.request, "urlopen", _urlopen)

    installed = caddy.install()
    assert installed == tmp_path / ".local" / "bin" / "caddy"
    assert installed.exists()
    assert installed.read_bytes() == caddy_bytes


def test_install_downloads_and_extracts_latest_release_zip_on_windows(
    monkeypatch, tmp_path
):
    from dbx_caddy import caddy

    monkeypatch.setenv("HOME", str(tmp_path))

    monkeypatch.setattr(caddy.platform, "system", lambda: "Windows")
    monkeypatch.setattr(caddy.platform, "machine", lambda: "AMD64")

    asset_name = "caddy_2.10.2_windows_amd64.zip"
    asset_url = "https://example.invalid/caddy.zip"
    release = {
        "tag_name": "v2.10.2",
        "assets": [{"name": asset_name, "browser_download_url": asset_url}],
    }

    caddy_bytes = b"MZ...\n"
    archive_bytes = _zip_bytes({"caddy.exe": caddy_bytes})

    def _urlopen(req, timeout=30):
        url = getattr(req, "full_url", req)
        if url == caddy._LATEST_RELEASE_URL:
            return _FakeHTTPResponse(json.dumps(release).encode("utf-8"))
        if url == asset_url:
            return _FakeHTTPResponse(archive_bytes)
        raise AssertionError(f"Unexpected urlopen URL: {url!r}")

    monkeypatch.setattr(caddy.urllib.request, "urlopen", _urlopen)

    installed = caddy.install()
    assert installed == tmp_path / ".local" / "bin" / "caddy.exe"
    assert installed.exists()
    assert installed.read_bytes() == caddy_bytes


def test_run_inline_config_writes_temp_and_cleans_up_and_stops_process(
    monkeypatch, tmp_path
):
    from dbx_caddy import caddy

    monkeypatch.setattr(caddy, "path", lambda *a, **k: pathlib.Path("/usr/bin/caddy"))

    created: dict[str, object] = {}

    class FakeProc:
        def __init__(self, cmd):
            self.cmd = cmd
            self._poll = None
            self.terminate_calls = 0
            self.kill_calls = 0
            self.wait_calls: list[float | None] = []

        def poll(self):
            return self._poll

        def terminate(self):
            self.terminate_calls += 1

        def kill(self):
            self.kill_calls += 1
            self._poll = -9

        def wait(self, timeout=None):
            self.wait_calls.append(timeout)
            if len(self.wait_calls) == 1:
                raise subprocess.TimeoutExpired(cmd=self.cmd, timeout=timeout)
            self._poll = 0
            return 0

    def _popen(cmd, *a, **k):
        created["proc"] = FakeProc(cmd)
        created["cmd"] = cmd
        return created["proc"]

    monkeypatch.setattr(caddy.subprocess, "Popen", _popen)

    inline = ':8080 { respond "ok" }\n'
    with caddy.run(inline, "watch", adapter="caddyfile") as proc:
        cmd = created["cmd"]
        assert cmd[0] == "/usr/bin/caddy"
        assert cmd[1:3] == ["run", "--config"]
        cfg_path = pathlib.Path(cmd[3])
        assert cfg_path.exists()
        assert cfg_path.read_text(encoding="utf-8") == inline
        assert "--watch" in cmd
        assert "--adapter" in cmd
        assert "caddyfile" in cmd
        assert proc is created["proc"]

    # After context exit: process stop attempted, and temp config removed.
    proc = created["proc"]
    assert proc.terminate_calls == 1
    assert proc.kill_calls == 1
    assert proc.wait_calls == [10.0, 10.0]
    cfg_path = pathlib.Path(created["cmd"][3])
    assert not cfg_path.exists()


def test_run_with_config_path_does_not_create_temp_file(monkeypatch, tmp_path):
    from dbx_caddy import caddy

    monkeypatch.setattr(caddy, "path", lambda *a, **k: pathlib.Path("/usr/bin/caddy"))

    class FakeProc:
        def __init__(self, cmd):
            self.cmd = cmd
            self._poll = 0

        def poll(self):
            return self._poll

        def terminate(self):
            raise AssertionError("terminate() should not be called when already exited")

        def kill(self):
            raise AssertionError("kill() should not be called when already exited")

        def wait(self, timeout=None):
            return 0

    captured: dict[str, object] = {}

    def _popen(cmd, *a, **k):
        captured["cmd"] = cmd
        return FakeProc(cmd)

    monkeypatch.setattr(caddy.subprocess, "Popen", _popen)

    cfg = tmp_path / "Caddyfile"
    cfg.write_text(':8080 { respond "ok" }\n', encoding="utf-8")

    with caddy.run(cfg, "watch") as proc:
        assert proc.cmd[3] == str(cfg)

    # config file should remain because it was caller-provided
    assert cfg.exists()
