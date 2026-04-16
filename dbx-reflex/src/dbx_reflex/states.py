from urllib.parse import urlencode

import reflex as rx
from lfp_logging import logs
from reflex.event import EventSpec

LOG = logs.logger()


# noinspection PyTypeChecker
def set_query_param(
    state: rx.State, name: str, value: str | None, replace: bool = True
) -> EventSpec | list:
    """Return a redirect event when a query param change is needed.

    Returns an empty list when no URL change is required so callers can use it
    directly from event handlers without triggering a navigation.
    """
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
