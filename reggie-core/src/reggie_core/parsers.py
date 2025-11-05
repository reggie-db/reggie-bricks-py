"""Parsing helpers for common environment and config value coercions."""

def parse_bool(value, default=False):
    """Coerce various representations to a boolean value with default fallback."""
    if isinstance(value, bool):
        return value
    elif isinstance(value, int):
        return bool(value)
    elif not isinstance(value, str):
        value = str(value)
    value = value.strip()
    if not value:
        return default
    if value.isdigit():
        return bool(int(value))
    return value.casefold() == "true"
