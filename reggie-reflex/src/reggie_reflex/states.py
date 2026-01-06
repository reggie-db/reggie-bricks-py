from urllib.parse import urlencode

import reflex as rx
from reflex.event import EventSpec
from reggie_core import logs

LOG = logs.logger(__name__)


# noinspection PyTypeChecker
def set_query_param(
    state: rx.State, name: str, value: str | None, replace: bool = True
) -> EventSpec:
    params = dict(state.router.url.query_parameters)
    modified = False
    if value:
        if name not in params or params[name] != value:
            params[name] = value
            modified = True

    else:
        if name in params:
            params.pop(name, None)
            modified = True

    if not modified:
        return []
    else:
        qs = urlencode(params)
        url = state.router.url.path + (f"?{qs}" if qs else "")
        LOG.info(f"redirect to: url{url} replace:{replace}")
        return rx.redirect(url, replace=replace)
