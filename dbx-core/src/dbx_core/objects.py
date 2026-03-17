"""Serialization helpers for dataclasses, objects, and JSON encoding."""

import datetime
import enum
import hashlib
import inspect
import json
from array import array
from collections import deque
from dataclasses import asdict, is_dataclass
from types import NoneType
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    TypeAlias,
    get_args,
    get_origin,
)

from lfp_logging import logs
from lfp_types import T

try:
    from pydantic import BaseModel
except ImportError:
    BaseModel = None

LOG = logs.logger()

DataType: TypeAlias = (
    None | bool | int | float | str | list["DataType"] | dict["DataType", "DataType"]
)
_DATA_TYPE_PRIMITIVE_TYPES = tuple(
    arg for arg in get_args(DataType) if arg is not NoneType and get_origin(arg) is None
)
_DATA_TYPE_TYPES = (
    tuple(
        get_origin(arg)
        for arg in get_args(DataType)
        if arg is not NoneType and get_origin(arg) is not None
    )
    + _DATA_TYPE_PRIMITIVE_TYPES
)
_COLLECTION_TYPES = (list, tuple, set, dict, frozenset, deque, array, range)


def _generate_attr_names(*function_names: Iterable[str]) -> list[str]:
    modifiers: list[Callable[[Iterable[str]], str]] = [
        lambda parts: "_".join(parts),
        lambda parts: "".join([parts[0]] + [p.capitalize() for p in parts[1:]]),
        lambda parts: "".join(parts),
    ]

    result = []
    for underscore in (False, True):
        for modifier in modifiers:
            for function_name_parts in function_names:
                function_name = modifier(function_name_parts)
                if underscore:
                    function_name = f"__{function_name}__"
                if function_name not in result:
                    result.append(function_name)
    return result


_DUMP_ATTRS = _generate_attr_names(
    ("model", "dump"), ("as", "dict"), ("to", "dict"), ("dict",)
)

_ENUM_ATTRS = ["name", "value"]


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


def attribute(obj: Any, *attrs: str, default: Any = None) -> Any:
    """
    Safely traverse a chain of attributes on an object.

    Iteratively applies ``getattr`` for each attribute name in ``attrs``.
    If at any point the object is ``None`` or an attribute does not exist,
    traversal stops and ``default`` is returned.

    Args:
        obj: The base object to traverse.
        *attrs: One or more attribute names to access in sequence.
        default: Value to return if any attribute in the chain is missing
            or resolves to ``None``. Defaults to ``None``.

    Returns:
        The final attribute value if all attributes exist and are not ``None``,
        otherwise ``default``.

    Examples:
        attribute(event, "part", "tool_name")
        attribute(user, "profile", "address", "city", default="unknown")
    """
    if obj is not None:
        for attr in attrs:
            obj = getattr(obj, attr, None)
            if obj is None:
                break
    return obj if obj is not None else default


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


def dump(obj: Any, recursive: bool = True, member_properties: bool = True) -> DataType:
    """Convert an object graph to JSON-like Python data structures.

    Serialization order:
    1. ``None``, mappings, collections, enums, and primitive data types are converted directly.
    2. For custom objects, common dump helpers are attempted in order:
       ``model_dump``, ``as_dict``, ``to_dict``, and ``dict`` (including dunder/camel variants).
    3. Dataclasses fall back to ``dataclasses.asdict``.
    4. Any remaining object is stringified with ``str(value)``.

    Args:
        obj: Object to serialize.
        recursive: If ``True``, recursively serialize nested values.
        member_properties: If ``True``, include class ``@property`` values and
            eligible class members via :func:`_properties`.

    Returns:
        A JSON-compatible data representation.
    """

    def _to_data(value: Any) -> DataType:
        if value is None:
            return None
        elif isinstance(value, Mapping):
            data = {}
            for k, v in value.items():
                data[_to_data(k) if recursive else k] = _to_data(v) if recursive else v
            return data
        elif isinstance(value, _COLLECTION_TYPES):
            return [_to_data(v) if recursive else v for v in value]
        elif isinstance(value, enum.Enum):
            return _to_data(value.value)
        elif isinstance(value, _DATA_TYPE_TYPES):
            return value
        else:
            data = None
            for dump_attr in _DUMP_ATTRS:
                fn = getattr(value, dump_attr, None)
                if not callable(fn):
                    continue
                try:
                    fn_result = fn()
                except TypeError:
                    continue
                if isinstance(fn_result, _DATA_TYPE_TYPES):
                    data = _to_data(fn_result)
                    break
            if data is None:
                if is_dataclass(value):
                    data = _to_data(asdict(value))
                else:
                    encoded, encoded_value = _encode_default(value)
                    if encoded:
                        data = _to_data(encoded_value)
            if data is None or (member_properties and isinstance(data, Mapping)):
                for k, v in _properties(value, member_properties=member_properties):
                    k = _to_data(k) if recursive else k
                    if data is None:
                        data = {}
                    elif k in data:
                        continue
                    data[k] = _to_data(v) if recursive else v
            if data is None:
                data = str(data)
            return data

    return _to_data(obj)


def to_list[T](obj: Iterable[T] | T, flatten: bool = False) -> list[T]:
    def _to_list(value: Iterable[T] | T) -> list:
        if isinstance(value, list):
            values = value
        elif isinstance(value, _COLLECTION_TYPES):
            values = list(value)
        else:
            return [value]
        if not values or not flatten:
            return values
        else:
            result = []
            for v in values:
                result.extend(_to_list(v))
            return result

    return _to_list(obj)


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

        for k, v in inspect.getmembers(obj.__class__):
            if (
                k
                and k not in keys
                and not k.startswith("_")
                and (
                    BaseModel is None
                    or not isinstance(obj, BaseModel)
                    or not k.startswith("model_")
                )
            ):
                if isinstance(v, property):
                    if not member_properties:
                        continue
                    fget = getattr(v, "fget", None)
                    if not callable(fget):
                        continue
                    v = fget(obj)
                elif callable(v) or (isinstance(obj, enum.Enum) and k in _ENUM_ATTRS):
                    continue
                keys.add(k)
                yield k, v


def _json_encode_default(o: Any) -> Any:
    """Default JSON encoding strategy covering dataclasses and ISO-like values."""
    encoded, encoded_value = _encode_default(o)
    if encoded:
        return encoded_value
    data = dump(o)
    if isinstance(data, dict):
        return data
    return str(o)


def _encode_default(o: Any) -> tuple[bool, Any]:
    if o is None or isinstance(o, _DATA_TYPE_PRIMITIVE_TYPES):
        return True, o
    isoformat_attr = getattr(o, "isoformat", None)
    isoformat = isoformat_attr() if callable(isoformat_attr) else None
    if isinstance(isoformat, str):
        return True, isoformat
    return False, None


class _JSONEncoder(json.JSONEncoder):
    """
    JSON encoder that handles ISO dates and objects with dump() support.

    Automatically converts objects with isoformat() methods and objects that can
    be converted via dump() to JSON-serializable formats.
    """

    def default(self, o: Any) -> Any:
        return _json_encode_default(o)


class _JSONMemberPropertiesEncoder(json.JSONEncoder):
    """
    JSON encoder that includes member properties when converting objects.

    Same as _JSONEncoder but uses dump() with member_properties=True to include
    class member attributes and @property values.
    """

    def default(self, o: Any) -> Any:
        return _json_encode_default(o)


if __name__ == "__main__":
    print(_DUMP_ATTRS)
    LOG.info(dump({"neat": 1, "cool": "wow"}))
    LOG.info(
        dump({"neat": 1, "cool": "wow", "suh": [1, 2, 3, datetime.datetime.now()]})
    )
    LOG.info(dump("ok"))
    LOG.info(hash({"neat": 1, "cool": "wow"}).hexdigest())
    LOG.info(hash({"cool": "wow", "neat": 1}).hexdigest())
    LOG.info(hash({"neat": 1, "cool": "wow"}).hexdigest())
    LOG.info(hash({"cool": "wow", "neat": 1}).hexdigest())
    LOG.info(hash({"neat": 1, "cool": "wow"}, sort_keys=False).hexdigest())
    LOG.info(hash({"cool": "wow", "neat": 1}, sort_keys=False).hexdigest())
    LOG.info(hash(1).hexdigest())
    LOG.info(hash("1").hexdigest())
    LOG.info(hash(datetime.datetime.now()).hexdigest())
    call(lambda x: LOG.info(f"suh {x}"), "wow", "cool", "neat")
    call(lambda x, y: LOG.info(f"suh {x} {y}"), "wow")
    print(list(_generate_attr_names(("to", "data"), ["as", "dict"])))
