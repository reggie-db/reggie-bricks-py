import re
from itertools import chain
from typing import Any, Iterable

"""Utilities for normalizing names/identifiers into token fragments.

These helpers accept mixed inputs (strings, numbers, None) and yield normalized
tokens useful for search, matching, or building canonical identifiers.
"""

_SPLIT_NON_ALPHA_NUMERIC = re.compile(r"[^a-zA-Z0-9]+")
_SPLIT_CAMEL_CASE = re.compile(r"(?<=[a-z])(?=[A-Z])")


def tokenize(
    *inputs: Any,
    whitespace: bool = True,
    non_alpha_numeric: bool = True,
    camel_case: bool = True,
    lower: bool = True,
    capitalize: bool = False,
) -> Iterable[str]:
    """Yield normalized token fragments from mixed input values.

    The pipeline is configurable:
    - Whitespace-only split (if `whitespace=True` and `non_alpha_numeric=False`)
    - Non-alphanumeric split (if `non_alpha_numeric=True`)
    - CamelCase split (if `camel_case=True`)
    - Case normalization (lower or capitalize)

    Args:
        inputs: Any values to tokenize. None and empty strings are ignored.
        whitespace: Split on whitespace only when non-alphanumeric splitting is disabled.
        non_alpha_numeric: Split on non-alphanumeric boundaries.
        camel_case: Split tokens at camelCase boundaries.
        lower: Convert tokens to lowercase.
        capitalize: Capitalize tokens (applied after lower).

    Yields:
        Token fragments as strings.

    Examples:
        >>> list(tokenize("HelloWorld"))
        ['hello', 'world']
        >>> list(tokenize("  foo_bar  ", non_alpha_numeric=True))
        ['foo', 'bar']
        >>> list(tokenize("OneTwo3", lower=False, capitalize=True))
        ['Onetwo3']
    """
    value_strs = _strings(*inputs)
    if whitespace and not non_alpha_numeric:
        value_strs = chain.from_iterable(s.split() for s in _strings(*inputs))
    elif non_alpha_numeric:
        value_strs = split_non_alpha_numeric(*value_strs)
    if camel_case:
        value_strs = split_camel_case(*value_strs)
    for value_str in value_strs:
        if lower:
            value_str = value_str.lower()
        if capitalize:
            value_str = value_str.capitalize()
        yield value_str


def split_non_alpha_numeric(*inputs: Any) -> Iterable[str]:
    """Split inputs on non-alphanumeric boundaries."""
    return chain.from_iterable(
        _SPLIT_NON_ALPHA_NUMERIC.split(s) for s in _strings(*inputs)
    )


def split_camel_case(*inputs: Any) -> Iterable[str]:
    """Split inputs on camelCase boundaries."""
    return chain.from_iterable(_SPLIT_CAMEL_CASE.split(s) for s in _strings(*inputs))


def _strings(*inputs: Any) -> Iterable[str]:
    """Yield non-empty, stripped string representations for inputs."""
    for value in inputs:
        value_str = str(value).strip() if value is not None else None
        if value_str:
            yield value_str
