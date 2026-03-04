from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import BaseModel

from dbx_core import objects


class Color(Enum):
    RED = "red"


@dataclass
class Point:
    x: int
    y: int


class HasToDict:
    def to_dict(self):
        return {"ok": True, "n": 1}


class HasProperty:
    base = 10

    @property
    def computed(self) -> int:
        return 42


class HasToDictAndProperty:
    @property
    def computed(self) -> int:
        return 7

    def to_dict(self):
        return {"ok": True}


class UserModel(BaseModel):
    name: str
    age: int


def test_dump_primitives_and_enum():
    assert objects.dump(None) is None
    assert objects.dump(1) == 1
    assert objects.dump("x") == "x"
    assert objects.dump(Color.RED) == "red"


def test_dump_dataclass():
    assert objects.dump(Point(1, 2)) == {"x": 1, "y": 2}


def test_dump_uses_to_dict_when_available():
    assert objects.dump(HasToDict()) == {"ok": True, "n": 1}


def test_dump_pydantic_model_without_member_properties():
    model = UserModel(name="reggie", age=32)
    assert objects.dump(model, member_properties=False) == {
        "name": "reggie",
        "age": 32,
    }


def test_dump_pydantic_model_with_member_properties_bug():
    model = UserModel(name="reggie", age=32)
    assert objects.dump(model) == {"name": "reggie", "age": 32}


def test_dump_member_properties_toggle():
    with_properties = objects.dump(HasToDictAndProperty(), member_properties=True)
    without_properties = objects.dump(HasToDictAndProperty(), member_properties=False)
    assert with_properties["computed"] == 7
    assert with_properties["ok"] is True
    assert "computed" not in without_properties


def test_dump_recursive_toggle():
    payload = {"a": [Point(1, 2)]}
    recursive = objects.dump(payload, recursive=True)
    non_recursive = objects.dump(payload, recursive=False)
    assert recursive == {"a": [{"x": 1, "y": 2}]}
    assert isinstance(non_recursive["a"][0], Point)


def test_dump_nested_custom_object_should_use_nested_to_dict():
    payload = {"nested": HasToDict()}
    assert objects.dump(payload) == {"nested": {"ok": True, "n": 1}}


def test_dump_nested_datetime_uses_isoformat_string():
    payload = {
        "neat": 1,
        "cool": "wow",
        "suh": [1, 2, 3, datetime(2026, 3, 4, 14, 39, 46, 1819)],
    }
    dumped = objects.dump(payload)
    assert dumped["suh"][3] == "2026-03-04T14:39:46.001819"
