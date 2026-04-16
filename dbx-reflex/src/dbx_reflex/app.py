import reflex as rx
from reflex.event import EventSpec

from dbx_reflex import events, states


class SortState(rx.State):
    """State helper for sort query parameter synchronization."""

    @rx.var
    def sort(self) -> str:
        """Return the current sort query parameter normalized to lowercase."""
        return self.router.url.query_parameters.get("sort", "asc").lower()

    def set_sort(self, value: str) -> EventSpec:
        """Set or clear the ``sort`` query parameter."""
        return states.set_query_param(self, "sort", value)


class SearchState(rx.State):
    """State helper for search query parameter synchronization."""

    page: str = "1"

    @rx.var
    def q(self) -> str:
        """Return the current ``q`` search query parameter."""
        return self.router.url.query_parameters.get("q", "")

    def set_q(self, value: str) -> EventSpec:
        """Set or clear the ``q`` query parameter."""
        return states.set_query_param(self, "q", value)


def index() -> rx.Component:
    """Render a demo page showing query-bound search and sort state."""
    search_input = rx.input(
        name="q",
        placeholder="Type query...",
        on_blur=SearchState.set_q,
        on_change=SearchState.set_q.debounce(2 * 1000),
        default_value=SearchState.q,
        on_key_down=events.on_key_down_value(SearchState.set_q, "Enter"),
    )
    return rx.vstack(
        rx.heading("Auto Bound Query Params"),
        rx.text(SearchState.q),
        rx.text(SortState.sort),
        rx.hstack(
            search_input,
            rx.select(
                ["asc", "desc"],
                value=SortState.sort,
                on_change=SortState.set_sort,
            ),
        ),
    )


app = rx.App()
app.add_page(index)
