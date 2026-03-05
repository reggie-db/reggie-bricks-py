from dbx_tools import runtimes


def test_version_reads_valid_pep440_runtime(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.3.2")
    assert str(runtimes.version()) == "15.3.2"


def test_version_coerces_client_prefixed_runtime(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.5.0")
    assert str(runtimes.version()) == "0.5.0+client"
