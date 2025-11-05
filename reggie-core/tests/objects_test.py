"""
Quick tests for `reggie_core.objects` moved from the source module.

This mirrors the original ad hoc prints from `objects.py` under a test location,
while explicitly referencing `objects.to_json` and `objects.to_dict`.
"""

import datetime

from reggie_core import objects


if __name__ == "__main__":
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


