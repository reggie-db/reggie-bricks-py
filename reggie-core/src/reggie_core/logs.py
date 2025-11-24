"""
Lightweight logging helpers.

Environment variables
- LOGGING_SERVER: if true, treat this process as a server even if a TTY exists
- LOGGING_PRINT: if true, INFO records on interactive terminals are routed via print
"""

import functools
import logging
import os
import platform
import sys
from collections.abc import Callable, Iterable

from reggie_core import inputs, parsers, paths

_LOGGING_AUTO_CONFIG = parsers.to_bool(os.getenv("LOGGING_AUTO_CONFIG", True))
_LOGGING_SERVER = parsers.to_bool(os.getenv("LOGGING_SERVER"))
_LOGGING_PRINT = parsers.to_bool(os.getenv("LOGGING_PRINT", True))
_AUTO_CONFIG_MARK = object()


def auto_config():
    if not _LOGGING_AUTO_CONFIG:
        return

    def _has_auto_config_handler():
        if handlers := logging.getLogger().handlers:
            for handler in handlers:
                if _AUTO_CONFIG_MARK is getattr(handler, "auto_config_mark", None):
                    return True
        return False

    if _has_auto_config_handler():
        return

    logging.basicConfig(level=logging.INFO, handlers=list(_auto_config_handlers()))

    if not _has_auto_config_handler():
        return

    logging_basic_config = logging.basicConfig

    def logging_basic_config_wrapper(*args, **kwargs):
        kwargs.get("logging_basic_config_wrapper")
        if not kwargs.get("force") and _has_auto_config_handler():
            kwargs["force"] = True
        logging_basic_config(*args, **kwargs)

    logging.basicConfig = logging_basic_config_wrapper


def logger(*names: str | None) -> logging.Logger:
    """
    Return a logger for the resolved name or the root logger.

    Selection rules
    1) Use the first non empty name that is not "__main__"
    2) If a provided name looks like a path, use its basename without extension
    3) If nothing resolves, return the root logger

    The returned logger does not configure handlers here. Use Handler and Formatter
    defined below when attaching handlers.
    """
    auto_config()
    name = _logger_name(*names)
    return logging.getLogger(name) if name else logging.getLogger()


def get_levels() -> Iterable[tuple[int, str]]:
    """
    Return all known logging levels as (value, name) pairs.

    Order is whatever logging._nameToLevel currently holds. Intended for UI
    population or diagnostics rather than program logic.
    """
    return [(val, name) for name, val in logging._nameToLevel.items()]


def get_level(level, default=None) -> tuple[int, str]:
    """
    Resolve a level specifier to a (value, name) pair.

    Accepts
    - int: treated as a level value
    - str: matches exact names first, then case insensitive match,
           then unambiguous case insensitive prefix

    If default is provided and resolution fails, default is resolved by the
    same rules and returned.

    Raises
    - ValueError on invalid or ambiguous string values when default is not given
    """
    if isinstance(level, int):
        name = logging._levelToName.get(level, None)
        if name:
            return level, name
    else:
        level = str(level).strip()
        val = logging._nameToLevel.get(level, None)
        if val:
            return val, level
    level_names = get_levels()
    for val, name in level_names:
        match = False
        if isinstance(level, int):
            match = val == level
        else:
            match = name.casefold() == level.casefold()
        if match:
            return val, name
    if not isinstance(level, int):
        matched: dict[str, int] = {}
        for val, name in level_names:
            if name.casefold().startswith(level.casefold()):
                matched.setdefault(name, val)
        if len(matched) == 1:
            name, val = matched.popitem()
            return val, name
        elif len(matched) > 1:
            raise ValueError(f"Ambiguous level: {level}")
    if default is not None:
        return get_level(default)
    raise ValueError(f"Invalid level: {level}")


def _logger_name(*names: str | None):
    """
    Resolve a logger name from a list of candidates.

    Skips falsy values and "__main__".
    If a candidate refers to a file, derive the module name from its basename.
    Returns None when no candidate resolves.
    """
    if names:

        def _is_name_valid(name: str) -> bool:
            return name and name != "__main__"

        for name in names:
            if not _is_name_valid(name):
                continue
            if file_path := paths.path(name):
                name = os.path.splitext(os.path.basename(file_path.name))[0]
                if not _is_name_valid(name):
                    continue
            return name
    return None


def _auto_config_handlers() -> Iterable[logging.Handler]:
    for error in [False, True]:
        stream = sys.stdout if not error else sys.stderr
        yield _auto_config_handler(stream, error)


def _auto_config_handler(stream, error: bool) -> logging.Handler:
    handler = Handler(stream=stream)
    handler.auto_config_mark = _AUTO_CONFIG_MARK
    handler.setFormatter(Formatter(stream=stream))
    if error:
        handler.setLevel(logging.WARN)
    else:
        handler.addFilter(lambda record: record.levelno < logging.WARNING)
    return handler


@functools.cache
def _is_system_account() -> int:
    """
    Heuristic: return True if this process runs as a system account.

    On Linux, accounts with uid < 1000 are often system accounts.
    Returns False on non Unix platforms or if the check fails.
    """
    try:
        import pwd

        if pwd.getpwuid(os.getuid()).pw_uid < 1000:
            return True
    except Exception:
        pass
    return False


def _is_server(stream=None) -> bool:
    """
    Return True if this process looks like a server or non interactive runtime.
    """
    # Explicit override via env var — allows forcing server behavior manually.
    if _LOGGING_SERVER:
        return True
    # Treat macOS and Windows as user machines; generally not headless servers.
    if platform.system() in ("Windows", "Darwin"):
        return False
    # Databricks notebooks expose ENABLE_REPL_LOGGING; if true, it's an interactive session.
    if parsers.to_bool(os.getenv("ENABLE_REPL_LOGGING")):
        return False
    # If stdout/stderr is not attached to a TTY, assume non-interactive (daemon, cron, etc.).
    if not inputs.is_interactive(stream):
        return True
    # Lack of DISPLAY or WAYLAND_DISPLAY means no GUI session; likely a headless server.
    if not (os.getenv("DISPLAY") or os.getenv("WAYLAND_DISPLAY")):
        return True
    # Typical CI, container, or system-managed process indicators.
    if any(
        os.getenv(k)
        for k in ("CI", "KUBERNETES_SERVICE_HOST", "CONTAINER", "SYSTEMD_EXEC_PID")
    ):
        return True
    # Fallback: treat system accounts (UID < 1000) as server processes.
    return _is_system_account()


class Handler(logging.StreamHandler):
    """
    StreamHandler with level based color and optional print routing.

    Features
    - Colors by level when the target stream supports ANSI color
    - On interactive terminals and when LOGGING_PRINT is true, INFO logs to stdout
      are emitted via print to leverage notebook and UI capture layers
    - Server detection influences formatting and routing
    """

    COLORS = {
        logging.DEBUG: "\033[38;2;180;180;180m",
        logging.INFO: None,
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[41m",
    }
    RESET = "\033[0m"

    def __init__(self, *args, stream=None, **kwargs):
        super().__init__(*args, stream=stream, **kwargs)
        self._server = _is_server(stream)

    def emit(self, record):
        if self._is_print(record):
            record.print = True
            print(self.format(record, True))
        else:
            super().emit(record)

    def format(self, record, disable_color=False):
        msg = super().format(record)
        if disable_color or not self._supports_color():
            return msg
        color = self._color(record.levelno)
        reset = self.RESET if color else ""
        return f"{color}{msg}{reset}"

    def _is_print(self, record: logging.LogRecord) -> bool:
        """
        Return True if this record should be routed via print.
        """
        if not _LOGGING_PRINT or sys.stderr == self.stream:
            return False
        return record.levelno == logging.INFO and not self._server

    def _supports_color(self) -> bool:
        """
        Return True if the handler stream likely supports ANSI colors.

        TTY and TERM are checked on Unix.
        On Windows, colorama presence or known terminals enable ANSI.
        """
        if not inputs.is_interactive(self.stream):
            return False
        term = os.getenv("TERM", "")
        for dumb_term in ("dumb", "", "unknown"):
            if dumb_term.casefold() == term.casefold():
                return False
        if os.name != "nt":
            return True
        if Handler._is_colorama():
            return True
        return (
            os.getenv("WT_SESSION")
            or os.getenv("ANSICON")
            or os.getenv("TERM_PROGRAM") == "vscode"
        )

    @staticmethod
    def _color(levelno):
        """
        Return the ANSI color code for the given level number.

        Uses an exact match when present in COLORS, otherwise selects the
        closest lower configured level.
        """
        if levelno in Handler.COLORS:
            color = Handler.COLORS.get(levelno, None)
        else:
            _, color = max(
                ((k, v) for k, v in Formatter.COLORS.items() if k <= levelno),
                key=lambda x: x[0],
            )
        return color or ""

    @staticmethod
    @functools.cache
    def _is_colorama() -> bool:
        """Return True if colorama is importable in this environment."""
        try:
            import colorama  # noqa: F401  # pyright: ignore[reportUnusedImport, reportMissingModuleSource]

            return True
        except ImportError:
            return False


class Formatter(logging.Formatter):
    """
    Space efficient formatter with optional timestamp, level, name and message.

    Behavior
    - In server mode, includes an ISO style timestamp
    - Suppresses the logger name when it is the root logger
    - Supports a transient record.print flag to suppress level and name when
      routing via print for cleaner user facing output

    Delimiters
    - Fields are separated by a single space
    - A trailing " | " is inserted before the message when any preceding
      fields are present
    """

    def __init__(self, *args, stream=None, **kwargs):
        """
        Create the formatter.

        stream controls server mode detection for timestamp inclusion.
        datefmt is fixed to YYYY-MM-DD HH:MM:SS for stable parsing.
        """
        super().__init__(*args, datefmt="%Y-%m-%d %H:%M:%S", **kwargs)
        self._formatters: list[Callable[[logging.LogRecord], str]] = []
        if _is_server(stream):
            self._formatters.append(lambda x: self.formatTime(x, self.datefmt))
        self._formatters.append(lambda x: x.levelname if not x.print else "")
        self._formatters.append(
            lambda x: f"[{x.name}]" if x.name and x.name != logging.root.name else ""
        )
        self._formatters.append(lambda x: x.message)

    def formatMessage(self, record):
        """
        Build the final message text by evaluating the configured field functions.

        record.print is initialized to False if missing. Empty field fragments are
        skipped. A single space separates fields, and a " | " separates any
        header fields from the message.
        """
        if not hasattr(record, "print"):
            setattr(record, "print", False)
        out = None
        formatters_len = len(self._formatters)
        for i in range(formatters_len):
            formatter = self._formatters[i]
            if value := formatter(record):
                if out:
                    delimiter = " " if i < formatters_len - 1 else " | "
                    out += delimiter + value
                else:
                    out = value
        return out or ""
