"""Parsing helpers for common environment and config value coercions."""

from dbx_core.strs import trim


def to_float(value, default=None) -> float | None:
    """Coerce values to float with default fallback."""
    if isinstance(value, float):
        return value
    elif isinstance(value, int):
        return float(value)
    elif value:
        if value_str := trim(value, default=None):
            try:
                return float(value_str)
            except ValueError:
                return default
    return default
