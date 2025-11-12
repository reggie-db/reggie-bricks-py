"""Serialization helpers for dataclasses, objects, and JSON encoding."""

import datetime
import hashlib
import inspect
import json
from array import array
from collections import deque
from typing import Any, Callable, TypeVar, Iterable

T = TypeVar("T")
_SENTINEL = object()
_DUMP_ATTRS = ["model_dump", "as_dict", "to_dict", "asDict", "toDict"]
_DESCRIPTOR_ATTRS = ["__get__", "__set__", "__delete__"]
_COLLECTION_TYPES = (list, tuple, set, dict, frozenset, deque, array, range)


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


def remove_keys(d: dict, *keys: str):
    def _remove_keys(obj: Any, *keys: str):
        if keys and isinstance(obj, dict) and obj:
            for key in keys:
                if key in obj:
                    del obj[key]
            for v in obj.values():
                _remove_keys(v, *keys)

    _remove_keys(d, *keys)


def dump(
        obj: Any, recursive: bool = True, member_properties: bool = True
) -> dict[Any, Any]:
    """Convert an object into a dictionary with optional property extraction.
    Dataclasses are expanded via asdict. Properties can be included by evaluating
    @property getters. When recursive is True, nested values are converted too.
    """

    def _dump(value: Any):
        if value is None:
            return value
        if isinstance(value, dict):
            if recursive:
                out = {}
                for k, v in value.items():
                    out[k] = _dump(v)
                return out
            else:
                return value
        elif isinstance(value, _COLLECTION_TYPES):
            if recursive:
                out = []
                for v in value:
                    out.append(_dump(v))
                return out
            else:
                return value
        else:
            for attr in _DUMP_ATTRS:
                if hasattr(value, attr):
                    dump_attr = getattr(value, attr)
                    v = dump_attr() if callable(dump_attr) else None
                    if v is not None:
                        return _dump(v) if recursive else v
            if hasattr(value, "__dict__"):
                v = value.__dict__
                if v is not None:
                    return _dump(v) if recursive else v
            out = None
            for k, v in _properties(value, member_properties):
                if out is None:
                    out = {}
                out[k] = _dump(v) if recursive else v
            return out if out is not None else value

    return _dump(obj)


def to_json(obj: Any, member_properties: bool = True, **kwargs) -> Any:
    """Serialize ``obj`` while automatically handling dataclasses and ISO dates."""
    kwargs.setdefault(
        "cls", _JSONMemberPropertiesEncoder if member_properties else _JSONEncoder
    )
    return json.dumps(obj, **kwargs)


def hash(
        obj: Any,
        sort_keys: bool = True,
        member_properties: bool = True,
        hash_fn: Callable[[bytes], T] = hashlib.sha256,
) -> T:
    obj_encoded = to_json(obj, sort_keys=sort_keys, member_properties=member_properties, separators=(",", ":")).encode()
    return hash_fn(obj_encoded)


def _properties(obj: Any, member_properties: bool = True) -> Iterable[tuple[str, Any]]:
    if obj is not None:
        keys = set()
        if member_properties:
            for k, v in inspect.getmembers(obj.__class__):
                if isinstance(v, property) and v.fget is not None and k not in keys:
                    keys.add(k)
                    yield k, v.fget(obj)

        for k, v in inspect.getmembers(obj.__class__):
            if not k.startswith("_") and k not in keys and not callable(v) and not any(
                    hasattr(v, m) for m in _DESCRIPTOR_ATTRS):
                keys.add(k)
                yield k, v


def _json_encoder_default(
        self: json.JSONEncoder, o: Any
) -> Any:
    """Default JSON encoding strategy covering dataclasses and ISO-like values."""
    if o is None:
        return None
    elif hasattr(o, "isoformat") and callable(o.isoformat):
        return o.isoformat()
    else:
        data = dump(o, recursive=False)
        if isinstance(data, dict):
            for k, v in data.items():
                data[k] = self.encode(v)
            return data
    return str(o)


class _JSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        return _json_encoder_default(self, o)


class _JSONMemberPropertiesEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        return _json_encoder_default(self, o)


if __name__ == "__main__":
    print(dump({"neat": 1, "cool": "wow"}))
    print(dump("ok"))
    print(hash({"neat": 1, "cool": "wow"}).hexdigest())
    print(hash({"cool": "wow", "neat": 1}).hexdigest())
    print(hash({"neat": 1, "cool": "wow"}).hexdigest())
    print(hash({"cool": "wow", "neat": 1}).hexdigest())
    print(hash({"neat": 1, "cool": "wow"}, sort_keys=False).hexdigest())
    print(hash({"cool": "wow", "neat": 1}, sort_keys=False).hexdigest())
    print(hash(1).hexdigest())
    print(hash("1").hexdigest())
    print(hash(datetime.datetime.now()).hexdigest())
    call(lambda x: print(f"suh {x}"), "wow", "cool", "neat")
    call(lambda x, y: print(f"suh {x} {y}"), "wow")
