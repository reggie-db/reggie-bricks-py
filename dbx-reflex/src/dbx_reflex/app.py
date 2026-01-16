import reflex as rx

from dbx_reflex import events, states


class SortState(rx.State):
    @rx.var
    def sort(self) -> str:
        return self.router.url.query_parameters.get("sort", "asc").lower()

    def set_sort(self, value: str):
        return states.set_query_param(self, "sort", value)


class SearchState(rx.State):
    page: str = "1"

    @rx.var
    def q(self) -> str:
        return self.router.url.query_parameters.get("q", "")

    def set_q(self, value: str):
        return states.set_query_param(self, "q", value)


def index():
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
