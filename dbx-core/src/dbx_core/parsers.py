"""Parsing helpers for common environment and config value coercions."""

from dbx_core.strs import trim

_BOOL_MAPPINGS = {
    True: ["true", "t", "yes", "y", "on"],
    False: ["false", "f", "no", "n", "off"],
}


def to_bool(value, default=False) -> bool:
    """Coerce various representations to a boolean value with default fallback."""
    if isinstance(value, bool):
        return value
    elif isinstance(value, int):
        return True if value == 1 else False if value == 0 else default
    elif not isinstance(value, str):
        value = trim(value)
    else:
        value = value.strip()
    if value:
        if value.isdigit():
            return to_bool(int(value), default)
        for bool_value, bool_strs in _BOOL_MAPPINGS.items():
            for bool_str in bool_strs:
                if value.casefold() == bool_str.casefold():
                    return bool_value
    return default


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
