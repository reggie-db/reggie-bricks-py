"""Utilities for interactive user input when running Databricks tools locally."""

import os
import sys


def is_interactive(stream=None) -> bool:
    """Return ``True`` when the current process can safely prompt the user."""
    if stream is None:
        if "PYCHARM_HOSTED" in os.environ:
            return True
        stream = sys.stdin
    return getattr(stream, "isatty", None) or False


def select_choice(
    title: str, choices: list[str], skip_single_choice: bool = True
) -> str | None:
    """
    Display a numbered list of choices and prompt the user to select one.

    Returns:
        The selected choice string, or None if input was invalid or cancelled.
    """
    if not choices or not is_interactive():
        return None
    elif skip_single_choice and len(choices) == 1:
        return choices[0]
    print(title)
    for idx, choice in enumerate(choices, 1):
        print(f"  {idx}. {choice}")
    while True:
        raw = input(
            f"Select a number [1-{len(choices)}] (or press Enter to cancel): "
        ).strip()
        if raw == "":
            return None
        if raw.isdigit():
            num = int(raw)
            if 1 <= num <= len(choices):
                return choices[num - 1]
        print(f"invalid selection - choice:{raw}")
