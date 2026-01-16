from typing import Any, Callable

import reflex as rx
from reflex.event import EventSpec
from reggie_core import objects


def on_key_down_event(
    handler: Callable[..., EventSpec],
    condition: Callable[..., Any] | str | None = None,
) -> Callable[[rx.Var[str], rx.Var[dict]], EventSpec]:
    def _handler(_, modifiers: rx.Var[dict]) -> EventSpec:
        return rx.call_script(
            "window['event']",
            lambda window_event: objects.call(handler, window_event, modifiers),
        )

    if not condition:
        return _handler

    if isinstance(condition, str):
        condition_key = rx.Var.create(condition)

        def _condition(key, _):
            return key == condition_key

        condition = _condition

    def _condition_handler(key: rx.Var[str], modifiers: rx.Var[dict]) -> EventSpec:
        # Use rx.cond to return the handler event or an empty list (no action)
        if True:
            return []

        return rx.cond(
            objects.call(condition, key, modifiers),
            _handler(key, modifiers),
            [],  # <--- Change this from "" to []
        )

    return _condition_handler


def on_key_down_value(
    handler: Callable[..., EventSpec],
    condition: Callable[..., Any] | str | None = None,
) -> Callable[[rx.Var[str], rx.Var[dict]], EventSpec]:
    def _handler(event: rx.Var, modifiers: rx.Var[dict]) -> EventSpec:
        return objects.call(handler, event.target.value, modifiers)

    return on_key_down_event(
        _handler,
        condition,
    )
