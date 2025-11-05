"""Serialization helpers for dataclasses, objects, and JSON encoding."""

import hashlib
import inspect
import json
from dataclasses import asdict, is_dataclass
from typing import Any, Callable, TypeVar

T = TypeVar("T")


def to_dict(
    obj: Any, properties: bool = True, recursive: bool = True
) -> dict[Any, Any]:
    """Convert an object into a dictionary with optional property extraction.
    Dataclasses are expanded via asdict. Properties can be included by evaluating
    @property getters. When recursive is True, nested values are converted too.
    """

    def _try_to_dict(value: Any) -> dict[Any, Any]:
        if isinstance(value, dict):
            d = value
        elif is_dataclass(value) and recursive:
            d = asdict(value)
        elif isinstance(value, object):
            d = getattr(value, "__dict__", None)
            if not isinstance(d, dict):
                return value
            d = d.copy()
        if properties:
            # Merge computed properties onto the dict view
            for member_name, member_value in _object_properties(value).items():
                d[member_name] = member_value
        if recursive:
            # Convert nested values in-place for a consistent mapping output
            for k, v in d.items():
                d[k] = _try_to_dict(v)
        return d

    data = _try_to_dict(obj)
    if data is None:
        return None
    elif not isinstance(data, dict):
        raise TypeError(f"Dict conversion failed, got {type(data)}")
    return data


def to_json(obj: Any, encode_properties: bool = False, **kwargs) -> Any:
    """Serialize ``obj`` while automatically handling dataclasses and ISO dates."""
    kwargs.setdefault(
        "cls", _JSONEncoderProperties if encode_properties else _JSONEncoder
    )
    return json.dumps(obj, **kwargs)


def hash(obj: Any, hash_fn: Callable[[bytes], T] = hashlib.sha256) -> T:
    json = to_json(obj, sort_keys=True, separators=(",", ":"))
    return hash_fn(json.encode())


def _object_properties(o: Any) -> dict[str, Any]:
    """Return a mapping of property names to evaluated values for an object."""
    properties = {}
    if isinstance(o, object):
        for k, v in inspect.getmembers(o.__class__):
            if isinstance(v, property) and v.fget is not None:
                properties[k] = v.fget(o)
    return properties


def _json_encoder_default(
    self: json.JSONEncoder, o: Any, encode_properties: bool = False
) -> Any:
    """Default JSON encoding strategy covering dataclasses and ISO-like values."""
    if o is None:
        return None
    elif hasattr(o, "isoformat") and callable(o.isoformat):
        return o.isoformat()
    elif is_dataclass(o) or isinstance(o, object):
        # Build a shallow dict view and encode nested values via encoder recursion
        data = to_dict(o, properties=encode_properties, recursive=False)
        for k, v in data.items():
            data[k] = self.encode(v)
        return data
    return str(o)


class _JSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        return _json_encoder_default(self, o)


class _JSONEncoderProperties(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        return _json_encoder_default(self, o, encode_properties=True)
