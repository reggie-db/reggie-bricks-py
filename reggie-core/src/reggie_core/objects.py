"""Serialization helpers for dataclasses, objects, and JSON encoding."""

import datetime
import hashlib
import inspect
import json
from dataclasses import asdict, is_dataclass
from typing import Any, Callable, Sequence, TypeVar

T = TypeVar("T")


def call(fn: Callable, *args: Any) -> Any:
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


def to_dict(
    obj: Any, properties: bool = True, recursive: bool = True, strict: bool = True
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
    elif strict and not isinstance(data, dict):
        raise TypeError(f"Dict conversion failed, got {type(data)}")
    return data


def to_json(obj: Any, encode_properties: bool = False, **kwargs) -> Any:
    """Serialize ``obj`` while automatically handling dataclasses and ISO dates."""
    kwargs.setdefault(
        "cls", _JSONEncoderProperties if encode_properties else _JSONEncoder
    )
    return json.dumps(obj, **kwargs)


def hash(
    obj: Any,
    sort_keys: bool = True,
    pickle: bool = False,
    hash_fn: Callable[[bytes], T] = hashlib.sha256,
) -> T:
    if pickle:
        if sort_keys:
            obj = _sort_keys(obj)
        obj_encoded = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        obj_encoded = to_json(obj, sort_keys=sort_keys, separators=(",", ":")).encode()
    return hash_fn(obj_encoded)


def _object_properties(o: Any) -> dict[str, Any]:
    """Return a mapping of property names to evaluated values for an object."""
    properties = {}
    if isinstance(o, object):
        for k, v in inspect.getmembers(o.__class__):
            if isinstance(v, property) and v.fget is not None:
                properties[k] = v.fget(o)
    return properties


def _sort_keys(obj: Any) -> Any:
    if isinstance(obj, Sequence) and not isinstance(obj, (str, bytes)):
        return tuple(_sort_keys(v) for v in obj)
    else:
        obj = to_dict(obj, strict=False)
        if isinstance(obj, dict):
            return tuple((k, _sort_keys(obj[k])) for k in sorted(obj))
    return obj


def _json_encoder_default(
    self: json.JSONEncoder, o: Any, encode_properties: bool = False
) -> Any:
    """Default JSON encoding strategy covering dataclasses and ISO-like values."""
    if o is None:
        return None
    elif hasattr(o, "isoformat") and callable(o.isoformat):
        return o.isoformat()
    elif is_dataclass(o) or isinstance(o, object):
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


if __name__ == "__main__":
    print(hash({"neat": 1, "cool": "wow"}).hexdigest())
    print(hash({"cool": "wow", "neat": 1}).hexdigest())
    print(hash({"neat": 1, "cool": "wow"}, sort_keys=False).hexdigest())
    print(hash({"cool": "wow", "neat": 1}, sort_keys=False).hexdigest())
    print(hash(1).hexdigest())
    print(hash("1").hexdigest())
    print(hash(datetime.datetime.now()).hexdigest())
    call(lambda x: print(f"suh {x}"), "wow", "cool", "neat")
    call(lambda x, y: print(f"suh {x} {y}"), "wow")
