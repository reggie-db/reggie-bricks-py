from dataclasses import dataclass


@dataclass(frozen=True)
class _W:
    id: str
    name: str
    cluster_size: str
    enable_serverless_compute: bool


class _Warehouses:
    def __init__(self, items):
        self._items = list(items)

    def list(self):
        return list(self._items)


class _WC:
    def __init__(self, items):
        self.warehouses = _Warehouses(items)


def test_warehouse_prefers_serverless_then_size_then_name():
    from dbx_tools import clients

    items = [
        _W(
            id="1",
            name="Shared Endpoint",
            cluster_size="Small",
            enable_serverless_compute=True,
        ),
        _W(id="2", name="demo", cluster_size="X-Large", enable_serverless_compute=True),
        _W(
            id="3",
            name="random biggest but not serverless",
            cluster_size="4X-Large",
            enable_serverless_compute=False,
        ),
        _W(
            id="4",
            name="zzz serverless biggest",
            cluster_size="2X-Large",
            enable_serverless_compute=True,
        ),
        _W(
            id="5",
            name="Shared Endpoint XL",
            cluster_size="X-Large",
            enable_serverless_compute=True,
        ),
    ]

    chosen = clients.warehouse(client=_WC(items))

    # serverless True wins over non-serverless, then largest size among serverless wins,
    # then name preference breaks ties within same size.
    assert chosen.id == "4"


def test_warehouse_name_preference_breaks_size_ties_within_serverless():
    from dbx_tools import clients

    items = [
        _W(
            id="1",
            name="zz top",
            cluster_size="X-Large",
            enable_serverless_compute=True,
        ),
        _W(
            id="2",
            name="demo",
            cluster_size="X-Large",
            enable_serverless_compute=True,
        ),
        _W(
            id="3",
            name="Shared Endpoint",
            cluster_size="X-Large",
            enable_serverless_compute=True,
        ),
    ]

    chosen = clients.warehouse(client=_WC(items))
    assert chosen.id == "3"


def test_warehouse_raises_when_none_found():
    from dbx_tools import clients

    class _EmptyWC:
        class _W:
            @staticmethod
            def list():
                return []

        warehouses = _W()

    try:
        clients.warehouse(client=_EmptyWC())
        assert False, "Expected ValueError"
    except ValueError:
        pass
