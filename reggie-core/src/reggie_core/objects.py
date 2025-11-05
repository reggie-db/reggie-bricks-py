import inspect
import json
from dataclasses import asdict, is_dataclass
from typing import Any


def to_dict(
    obj: Any, properties: bool = True, recursive: bool = True
) -> dict[Any, Any]:
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
            for member_name, member_value in _object_properties(value).items():
                d[member_name] = member_value
        if recursive:
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


def _object_properties(o: Any) -> dict[str, Any]:
    properties = {}
    if isinstance(o, object):
        for k, v in inspect.getmembers(o.__class__):
            if isinstance(v, property) and v.fget is not None:
                properties[k] = v.fget(o)
    return properties


def _json_encoder_default(
    self: json.JSONEncoder, o: Any, encode_properties: bool = False
) -> Any:
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
