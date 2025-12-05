import reflex as rx
import asyncio
import random
import string

from reggie_reflex.fragments import fragment_field, fragment_bound


@fragment_bound
class StreamState(rx.State):
    count: int = fragment_field(5)
    threshold: float = fragment_field(2.5)
    mode: str = fragment_field("auto")
    tags: list[str] = fragment_field([])
    levels: list[int] = fragment_field([1, 2, 3])

    items: list[str] = []
    running: bool = False
    _n_tasks: int = 0

    def mk(self):
        letters = string.ascii_letters
        return "".join(random.choice(letters) for _ in range(10))

    @rx.event
    def start(self):
        if not self.running:
            self.running = True
            return StreamState.run_stream

    @rx.event
    def cancel(self):
        self.running = False

    @rx.event(background=True)
    async def run_stream(self):
        async with self:
            if self._n_tasks > 0:
                return
            self._n_tasks += 1

        for _ in range(self.count):
            async with self:
                if not self.running:
                    self._n_tasks -= 1
                    return
                self.items.append(self.mk())

            await asyncio.sleep(1)

        async with self:
            self.running = False
            self._n_tasks -= 1


def index():
    return rx.vstack(
        rx.button("Start", on_click=StreamState.start),
        rx.button("Cancel", on_click=StreamState.cancel),
        rx.text("Running"),
        rx.text(StreamState.running),
        rx.text("Items"),
        rx.foreach(StreamState.items, lambda x: rx.text(x)),
    )


app = rx.App()
app.add_page(index)
