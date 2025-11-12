"""Serialization helpers for dataclasses, objects, and JSON encoding."""

import datetime
import hashlib
import inspect
import json
from array import array
from collections import deque
from typing import Any, Callable, Iterable, TypeVar

T = TypeVar("T")
_SENTINEL = object()
_DUMP_ATTRS = ["model_dump", "as_dict", "to_dict", "asDict", "toDict"]
_DESCRIPTOR_ATTRS = ["__get__", "__set__", "__delete__"]
_COLLECTION_TYPES = (list, tuple, set, dict, frozenset, deque, array, range)


def call(fn: Callable, *args: Any) -> Any:
    """
    Call a function with the provided arguments, padding with None if needed.

    If the function accepts *args (VAR_POSITIONAL), all arguments are passed through.
    Otherwise, only the number of fixed positional parameters are passed, with None
    used to pad if fewer arguments are provided than required.

    Args:
        fn: Callable function to invoke
        *args: Arguments to pass to the function

    Returns:
        Result of calling fn with the adjusted arguments
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


def remove_keys(d: dict, *keys: str):
    """
    Recursively remove specified keys from a dictionary and all nested dictionaries.

    Modifies the dictionary in place, traversing nested dictionaries to remove
    the specified keys at all levels.

    Args:
        d: Dictionary to modify
        *keys: Keys to remove from the dictionary and all nested dictionaries
    """

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
    """
    Convert an object into a dictionary with optional property extraction.

    Attempts multiple strategies to convert an object to a dictionary:
    1. Checks for common dump methods (model_dump, as_dict, to_dict, etc.)
    2. Uses __dict__ if available
    3. Extracts class members (properties and non-callable attributes)

    When recursive is True, nested objects are also converted. When member_properties
    is True, @property values are evaluated and included.

    Args:
        obj: Object to convert to dictionary
        recursive: If True, recursively convert nested objects
        member_properties: If True, include @property values and class members

    Returns:
        Dictionary representation of the object, or the original object if conversion
        fails and it's not a collection type
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
    """
    Serialize an object to JSON while automatically handling dataclasses and ISO dates.

    Uses custom JSON encoders that handle:
    - Objects with isoformat() method (datetime, date, etc.)
    - Objects that can be converted via dump()
    - Nested structures

    Args:
        obj: Object to serialize
        member_properties: If True, use encoder that includes member properties
        **kwargs: Additional arguments passed to json.dumps()

    Returns:
        JSON string representation of the object
    """
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
    """
    Hash an object using JSON serialization.

    Converts the object to JSON (with optional key sorting) and then hashes the
    resulting bytes. This provides deterministic hashing for dictionaries and
    other serializable objects.

    Args:
        obj: Object to hash
        sort_keys: If True, sort dictionary keys before hashing for deterministic output
        member_properties: If True, include member properties in serialization
        hash_fn: Hash function to use (defaults to hashlib.sha256)

    Returns:
        Hash object (result of hash_fn) with methods like hexdigest()
    """
    obj_encoded = to_json(
        obj,
        sort_keys=sort_keys,
        member_properties=member_properties,
        separators=(",", ":"),
    ).encode()
    return hash_fn(obj_encoded)


def _properties(obj: Any, member_properties: bool = True) -> Iterable[tuple[str, Any]]:
    """
    Extract properties and class members from an object.

    Yields tuples of (name, value) for:
    - @property attributes (if member_properties is True)
    - Non-private class attributes that are not callable or descriptors

    Args:
        obj: Object to extract properties from
        member_properties: If True, evaluate and include @property values

    Yields:
        Tuples of (property_name, property_value)
    """
    if obj is not None:
        keys = set()
        if member_properties:
            for k, v in inspect.getmembers(obj.__class__):
                if isinstance(v, property) and v.fget is not None and k not in keys:
                    keys.add(k)
                    yield k, v.fget(obj)

        for k, v in inspect.getmembers(obj.__class__):
            if (
                not k.startswith("_")
                and k not in keys
                and not callable(v)
                and not any(hasattr(v, m) for m in _DESCRIPTOR_ATTRS)
            ):
                keys.add(k)
                yield k, v


def _json_encoder_default(self: json.JSONEncoder, o: Any) -> Any:
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
    """
    JSON encoder that handles ISO dates and objects with dump() support.

    Automatically converts objects with isoformat() methods and objects that can
    be converted via dump() to JSON-serializable formats.
    """

    def default(self, o: Any) -> Any:
        return _json_encoder_default(self, o)


class _JSONMemberPropertiesEncoder(json.JSONEncoder):
    """
    JSON encoder that includes member properties when converting objects.

    Same as _JSONEncoder but uses dump() with member_properties=True to include
    class member attributes and @property values.
    """

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
