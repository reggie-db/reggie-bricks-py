import re
from typing import Any, Iterable


def tokenize(
    *inputs: Any, non_alpha_numeric=True, camel_case=True, lower=True
) -> Iterable[str]:
    for input in inputs:
        if input is None:
            continue
        elif not isinstance(input, str):
            input = str(input)
        input = input.strip()
        if not input:
            continue
        alpha_numeric_parts = (
            re.split(r"[^a-zA-Z0-9]+", input) if non_alpha_numeric else [input]
        )
        for alpha_numeric_part in alpha_numeric_parts:
            camel_parts = (
                re.split(r"(?<=[a-z])(?=[A-Z])", alpha_numeric_part)
                if camel_case
                else [alpha_numeric_part]
            )
            for camel_part in camel_parts:
                if camel_part:
                    if lower:
                        camel_part = camel_part.lower()
                    yield camel_part
