"""Callable invocation helpers shared across dbx-core consumers.

Includes:

* :func:`call` - invoke a callable with positional-arg padding/dropping so the
  caller does not need to know the target's exact arity.
* :func:`call_synchronized` / :func:`call_synchronized_async` - canonical
  double-checked locking idiom for lazy initialization, in both sync and
  asyncio flavors.
"""

import inspect
from typing import Any, Awaitable, Callable

from lfp_types import T


def call(fn: Callable, *args: Any) -> Any:
    """
    Call a function with the provided arguments, padding with None if needed.

    If the function accepts ``*args`` (VAR_POSITIONAL), all arguments are passed
    through. Otherwise, only the number of fixed positional parameters are
    passed, with ``None`` used to pad if fewer arguments are provided than
    required, or extra arguments dropped if more are provided.

    Args:
        fn: Callable function to invoke.
        *args: Arguments to pass to the function.

    Returns:
        Result of calling ``fn`` with the adjusted arguments.
    """
    sig = inspect.signature(fn)
    params = list(sig.parameters.values())

    if any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params):
        return fn(*args)

    fixed = [
        p
        for p in params
        if p.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    needed = len(fixed)

    adj = list(args[:needed])
    if len(adj) < needed:
        adj = adj + [None] * (needed - len(adj))

    return fn(*adj)


async def maybe_await(value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return await value
    return value
