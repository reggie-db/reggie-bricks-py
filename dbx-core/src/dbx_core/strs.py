import re
import textwrap
from itertools import chain
from typing import Any, Iterable, overload

"""Utilities for normalizing names/identifiers into token fragments.

These helpers accept mixed inputs (strings, numbers, None) and yield normalized
tokens useful for search, matching, or building canonical identifiers.
"""

_SPLIT_NON_ALPHA_NUMERIC = re.compile(r"[^a-zA-Z0-9]+")
_SPLIT_CAMEL_CASE = re.compile(r"(?<=[a-z])(?=[A-Z])")


@overload
def trim(value: Any, default: str = "", dedent: bool = True) -> str: ...


@overload
def trim(value: Any, default: None, dedent: bool = True) -> str | None: ...


def trim(value: Any, default: str | None = "", dedent: bool = True) -> str | None:
    """
    Normalize a value by optionally dedenting, stripping surrounding whitespace,
    and returning a fallback value when the result is empty.

    Args:
        value:
            The input value to normalize. Non-string inputs are converted to
            strings. If None or empty after processing, the function returns
            the provided default.

        default:
            Value returned when the processed string is empty or when the input
            is None. Defaults to an empty string.

        dedent:
            If True and the input is a string, remove common leading indentation
            using ``textwrap.dedent`` before stripping whitespace. Trailing
            whitespace is removed before the final ``strip`` call.

    Returns:
        The cleaned string, or ``default`` if the result is empty. When
        ``default`` is omitted or is a :class:`str`, the return type is
        :class:`str`; when ``default`` is ``None``, the return type may be
        ``None`` (see overloads for static analysis).
    """
    if value is not None:
        if not isinstance(value, str):
            value = str(value)
        if dedent:
            value = textwrap.dedent(value)
        value = value.strip()
    return value or default


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
        if value_str := trim(value, default=None):
            yield value_str
