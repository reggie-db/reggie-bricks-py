"""
Quick tests for `reggie_core.objects` moved from the source module.

This mirrors the original ad hoc prints from `objects.py` under a test location,
while explicitly referencing `objects.to_json` and `objects.to_dict`.
"""

import _collections_abc as cabc
import abc
import datetime

from reggie_core import objects


class NonStringIterable(metaclass=abc.ABCMeta):
    __slots__ = ()

    @abc.abstractmethod
    def __iter__(self):
        while False:
            yield None

    @classmethod
    def __subclasshook__(cls, c):
        if cls is NonStringIterable:
            if issubclass(c, str) or issubclass(c, bytes):
                return False
            return cabc._check_methods(c, "__iter__")
        return NotImplemented


if __name__ == "__main__":
    if True:
        typs = ["string", "string".encode(), iter(""), list(), dict(), tuple(), set()]
        print([isinstance(o, NonStringIterable) for o in typs])
    print(objects.to_json(None))
    print(objects.to_json(1))
    print(objects.to_json("1"))
    print(objects.to_json(datetime.datetime.now()))

    class Test:
        def __init__(self, a: int, b: int):
            self.a = a
            self.b = b

        @property
        def c(self) -> int:
            return self.a + self.b

    test = Test(1, 2)
    print(objects.to_dict(test))
    print(objects.to_dict(test, properties=False))
    print(objects.to_json(test))
    print(objects.to_json(test, encode_properties=True))
