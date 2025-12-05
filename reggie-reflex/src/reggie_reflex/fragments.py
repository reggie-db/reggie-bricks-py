import reflex as rx
from urllib.parse import parse_qs, urlencode
from typing import get_origin, TypeVar
from pydantic import TypeAdapter
import json

from reggie_core import logs

T = TypeVar("T")
LOG = logs.logger(__file__)

_FRAGMENT_MARKER = object()  # sentinel
_FRAGMENT_INFO_ATTR = "__fragment_info__"


def fragment_field(default):
    return (_FRAGMENT_MARKER, default)


def _parse_fragment_map(text):
    if not text:
        return {}
    return parse_qs(text.lstrip("#"), keep_blank_values=True)


def _encode_fragment_map(mapping):
    if not mapping:
        return ""
    return "#" + urlencode(mapping, doseq=True)


def _is_list_type(tp):
    return get_origin(tp) is list  # noqa: E721


def _auto_parse_string(value):
    v = value.strip()
    if v.startswith("[") and v.endswith("]"):
        try:
            return json.loads(v)
        except Exception:
            return value
    if v.startswith("{") and v.endswith("}"):
        try:
            return json.loads(v)
        except Exception:
            return value
    return value


def _decode_value(raw, expected_type):
    parsed = _auto_parse_string(raw)
    adapter = TypeAdapter(expected_type)
    try:
        return adapter.validate_python(parsed)
    except Exception:
        LOG.debug("failed to parse %r as %r", parsed, expected_type)
        return None


def _validate_default(name, default_value, expected_type):
    adapter = TypeAdapter(expected_type)
    try:
        adapter.validate_python(default_value)
    except Exception:
        raise TypeError(
            f"Default value {default_value!r} for field {name} does not match {expected_type}"
        )


def fragment_bound(_cls=None):
    def decorator(cls):
        fragment_fields = {}

        for name, expected_type in cls.__annotations__.items():
            value = getattr(cls, name, None)

            if (
                not isinstance(value, tuple)
                or len(value) != 2
                or value[0] is not _FRAGMENT_MARKER
            ):
                continue

            default_value = value[1]
            _validate_default(name, default_value, expected_type)

            fragment_fields[name] = default_value
            setattr(cls, name, default_value)

        setattr(cls, _FRAGMENT_INFO_ATTR, fragment_fields)

        def load_from_fragment(self):
            mapping = _parse_fragment_map(self.router.fragment)
            for name, default in fragment_fields.items():
                if name not in mapping:
                    continue
                raw_values = mapping[name]
                if not raw_values:
                    continue
                raw = raw_values[0]
                decoded = _decode_value(raw, cls.__annotations__[name])
                setattr(self, name, decoded)

        cls.load_from_fragment = load_from_fragment

        for name in fragment_fields:

            def make_setter(field):
                def setter(self, v):
                    setattr(self, field, v)

                    expected = cls.__annotations__[field]
                    adapter = TypeAdapter(expected)

                    if isinstance(v, (list, dict)):
                        encoded = json.dumps(v)
                    else:
                        try:
                            adapter.validate_python(v)
                            encoded = str(v)
                        except Exception:
                            encoded = str(v)

                    mapping = _parse_fragment_map(self.router.fragment)
                    mapping[field] = [encoded]
                    return rx.set_value("location.hash", _encode_fragment_map(mapping))

                return setter

            setattr(cls, f"set_{name}_fragment", make_setter(name))

        def on_load(self):
            self.load_from_fragment()

        cls.on_load = on_load

        return cls

    return decorator
