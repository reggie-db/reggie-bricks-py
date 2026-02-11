import email
import json
import pathlib
import re
import subprocess
import threading
from contextlib import contextmanager
from datetime import datetime
from email import policy as email_policy
from email.message import EmailMessage as StdlibEmailMessage
from email.parser import BytesParser
from functools import cached_property
from http.client import HTTPMessage
from http.server import BaseHTTPRequestHandler, HTTPServer
from tempfile import TemporaryDirectory
from typing import Iterator
from urllib.parse import unquote, urlparse

from lfp_logging import logs
from pydantic import BaseModel, ConfigDict, Field, computed_field

from dbx_ai import memory

LOG = logs.logger()

_DEFAULT_ACCOUNT = 1
_DEFAULT_MAILBOX = "INBOX"
_MAILBOX_MAPPINGS = {"sent": "Sent", "inbox": _DEFAULT_MAILBOX}
_ERROR_PREFIX = "ERROR:"
_MAIL_SOURCE_CACHE_TYPE = "mail_source_cache"
_UNICODE_SPACES_RE = re.compile(
    r"[\u00A0\u202F\u2007]"
)  # nbsp, narrow-nbsp, figure space
_UNICODE_MARKS_RE = re.compile(
    r"[\u200E\u200F\u202A-\u202E\u2066-\u2069]"
)  # bidi marks
_WHITESPACE_RE = re.compile(r"\s+")


class EmailMessageMetadata(BaseModel):
    account: int = Field(..., description="Account index (1-based).")
    mailbox: str = Field(
        ..., description='Mailbox name within the account (example: "INBOX").'
    )
    id: str = Field(
        ..., description="Message identifier (as returned by the mail provider)."
    )
    subject: str = Field(
        ..., description="Message subject line. Empty string if missing."
    )
    sender: str = Field(
        ...,
        description="Message sender (display name and/or email). Empty string if missing.",
    )
    received: datetime = Field(..., description="Message received timestamp.")
    is_read: bool = Field(..., description="True if the message is marked as read.")


class EmailMessage(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    account: int = Field(..., description="Account index (1-based).")
    mailbox: str = Field(
        ..., description='Mailbox name within the account (example: "INBOX").'
    )
    raw: StdlibEmailMessage = Field(
        ...,
        description="Raw message object (parsed by stdlib email package).",
        repr=False,
    )

    @computed_field(
        description='The "Message-ID" header value extracted from the email, if present.'
    )
    @cached_property
    def message_id(self) -> str | None:
        return _extract_message_id(self.raw)

    @computed_field(
        description="Decoded message body. Prefers text/plain, then text/html, otherwise empty string."
    )
    @cached_property
    def body(self) -> str:
        return _extract_body(self.raw)


class _MailOutput(dict[str, list[str]]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __repr__(self) -> str:
        return f"_MailOutput({dict(self)!r})"

    def file_name(self) -> str:
        return self.get_value("file_name", non_empty=True)

    def message_id(self) -> str:
        return self.get_headers().get("message-id", "")

    def get_headers(self) -> HTTPMessage:
        header_names = self.get_values("header_name")
        header_values = self.get_values("header_value")
        http_message = HTTPMessage()
        for name, value in zip(header_names, header_values):
            http_message.add_header(name, value)
        return http_message

    def get_value(self, key: str, **kwargs) -> str | None:
        for value in self.get_values(key, **kwargs):
            return value
        return None

    def get_values(
        self, key: str, strip: bool = True, non_empty: bool = False
    ) -> Iterator[str]:
        values = super().get(key, None)
        if values:
            for value in values:
                if strip:
                    value = value.strip()
                if non_empty and not value:
                    continue
                yield value

    def append(self, line: str) -> bool:
        if line:
            line = line.strip()
        if not line:
            return False
        if line == "---":
            return True
        parts = line.split("=", 1)
        if len(parts) == 2:
            if key := parts[0].lower().strip():
                value = parts[1].strip()
                if key == "error":
                    raise ValueError(f"Mail script error: {value!r}")
                self.setdefault(key, []).append(value)
                return False
        raise ValueError(f"Unexpected line: {line!r}")


def messages(
    account: int = _DEFAULT_ACCOUNT, mailbox: str = _DEFAULT_MAILBOX
) -> Iterator[EmailMessage]:
    mailbox = _normalize_mailbox(mailbox)
    for eml_msg in _eml_messages(account, mailbox):
        yield EmailMessage(
            account=account,
            mailbox=mailbox,
            raw=eml_msg,
        )


def _eml_messages(account: int, mailbox: str) -> Iterator[StdlibEmailMessage]:
    with TemporaryDirectory(delete=False) as td:
        tmp_dir = pathlib.Path(td)

        for output in _run_messages(account, mailbox, tmp_dir):
            print(f"Output: {output}")
            eml_path = None
            try:
                file_name = output.get_value("file_name")
                if file_name:
                    eml_path = tmp_dir / (file_name + ".eml")
                    with eml_path.open("rb") as f:
                        yield BytesParser(policy=email_policy.default).parse(f)
                else:
                    doc = memory.kv_read(_MAIL_SOURCE_CACHE_TYPE, output.message_id())
                    yield BytesParser(policy=email_policy.default).parsebytes(doc)
            finally:
                if eml_path:
                    eml_path.unlink()


def _run_messages(
    account: int, mailbox: str, tmp_dir: pathlib.Path
) -> Iterator[_MailOutput]:
    LOG.info(f"Writing messages - tmp_dir:%s", tmp_dir)
    # language=AppleScript
    script = r"""
    on run argv
        set accountIndex to (item 1 of argv) as integer
        set mailboxName to item 2 of argv
        set outDir to POSIX file (item 3 of argv)
        set cachePort to item 4 of argv

        tell application "Mail"
            set theAccount to account accountIndex
            set theMailbox to mailbox mailboxName of theAccount

            repeat with aMessage in messages of theMailbox
                set messageID to missing value
                set f to missing value
                set outPath to missing value
                try 
                    set msgHeaders to headers of aMessage
                    set msgHeaders to headers of aMessage
                    repeat with h in msgHeaders
                        set hname to name of h
                        ignoring case
                            if hname is "message-id" then
                                if messageID is missing value then
                                    set messageID to content of h
                                end if
                            end if
                        end ignoring
                        set hname to name of h
                        log "header_name=" & hname
                        set hvalue to content of h
                        log "header_value=" & hvalue
                    end repeat
                    -- if no message-id we must fetch
                    set shouldFetch to true
    
                    if messageID is not missing value then
                        log "message_id=" & messageID
                        set msgCached to false
                        set checkURL to "http://127.0.0.1:" & cachePort & "/" & messageID
                        
                        try
                            set result to do shell script "curl -fsS --max-time 2 " & quoted form of checkURL
                            if result is "true" then
                                set shouldFetch to false
                                set msgCached to true
                            end if
                        end try
                        log "cached=" & msgCached
                    end if
    
                    if shouldFetch is true then
                        -- random filename
                        set fileName to do shell script "uuidgen"
                        set outPath to (outDir as text) & fileName & ".eml"
    
                        set msgSource to source of aMessage
    
                        set f to open for access file outPath with write permission
                        set eof of f to 0
                        write msgSource to f  
                        log "file_name=" & fileName
                    end if
                    log "---"
                on error errMsg number errNum
                    if f is not missing value then
                        try
                            close access f
                        end try
                    end if

                    log "error=" & errNum & "|" & errMsg
                    error errMsg number errNum
                end try
            end repeat
        end tell
    end run
    """
    with _cache_server() as cache_port:
        process = subprocess.Popen(
            ["osascript", "-", str(account), mailbox, str(tmp_dir), str(cache_port)],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        process.stdin.write(script)
        process.stdin.close()

        outputs: list[_MailOutput] = []

        try:
            output = _MailOutput()
            for line in process.stderr:
                if output.append(line):
                    outputs.append(output)
                    file_name = output.get_value("file_name")
                    if file_name:
                        file_path = tmp_dir / (file_name + ".eml")
                        file_content = file_path.read_bytes()
                        memory.kv_write(
                            _MAIL_SOURCE_CACHE_TYPE, output.message_id(), file_content
                        )
                    yield output
                    output = _MailOutput()
        finally:
            process.stderr.close()
            if process.returncode is None:
                try:
                    process.terminate()
                    process.wait(timeout=10)
                finally:
                    if process.returncode is None:
                        process.kill()
                        process.wait()


class CacheHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            cached = False

            # expect: /<message-id>
            if parsed.path and parsed.path != "/":
                message_id = unquote(parsed.path[1:])
                cached = memory.kv_exists(_MAIL_SOURCE_CACHE_TYPE, message_id)

            body = json.dumps(cached).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()

            self.wfile.write(body)
            self.wfile.flush()

        except (BrokenPipeError, ConnectionResetError):
            # also ignore if it occurs before headers
            pass

    def log_message(self, *args):
        pass


@contextmanager
def _cache_server():
    server = HTTPServer(("127.0.0.1", 0), CacheHandler)  # random port
    port = server.server_address[1]

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        yield port
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def message(
    id: str, account: int = _DEFAULT_ACCOUNT, mailbox: str = _DEFAULT_MAILBOX
) -> EmailMessage:
    """
    Fetch and parse a single message.

    Notes:
    - The returned model includes only `message_id` from the headers.
    - Attachments are excluded from the body extraction.
    """
    msg = message_source(id, account=account, mailbox=mailbox)
    return EmailMessage(
        account=account,
        mailbox=mailbox,
        id=id,
        message_id=_extract_message_id(msg),
        body=_extract_body(msg),
    )


def message_source(
    msg_id: str, account: int = _DEFAULT_ACCOUNT, mailbox: str = _DEFAULT_MAILBOX
) -> StdlibEmailMessage:
    raw_mime = _run_message_source(msg_id, account, mailbox)
    return email.message_from_string(raw_mime, policy=email_policy.default)


def _run_message_source(msg_id: str, account: int, mailbox: str):
    if not (msg_id.startswith("<") and msg_id.endswith(">")):
        msg_id = f"<{msg_id}>"
    mailbox = _normalize_mailbox(mailbox)
    script = f"""
    tell application "Mail"
    set theInbox to mailbox "{mailbox}" of account {account}

    set foundMsgs to (every message of theInbox whose header contains "{msg_id}")

    if foundMsgs is not {{}} then
        set theMsg to item 1 of foundMsgs
        return id of theMsg
    else
        return ""
    end if
    end tell
    """

    process = subprocess.run(
        ["osascript", "-e", script], stdout=subprocess.PIPE, text=True, check=True
    )

    return process.stdout.strip() or None


def _run_osascript():
    pass


def _normalize_mailbox(mailbox: str) -> str:
    return _MAILBOX_MAPPINGS.get(mailbox.lower(), mailbox)


def _extract_message_id(msg: StdlibEmailMessage) -> str | None:
    """
    Extract the Message-ID header value.

    This intentionally excludes all other headers.
    """
    value = msg.get("Message-ID")
    if value is None:
        return None
    v = str(value).strip()
    return v or None


def _extract_body(msg: StdlibEmailMessage) -> str:
    """
    Extract the message body, preferring plain text then HTML.

    Attachments are excluded by using the structured body accessor.
    """
    part = msg.get_body(preferencelist=("plain",))
    if part is None:
        part = msg.get_body(preferencelist=("html",))
    if part is None:
        return ""
    return part.get_content() or ""


def _parse_mail_date(value: str) -> datetime:
    """
    Parse a mail client's `date received ... as string` format.

    Example:
      "Friday, February 6, 2026 at 6:20:06 PM"
    """

    def _normalize(s: str) -> str:
        s = _UNICODE_MARKS_RE.sub("", s)
        s = _UNICODE_SPACES_RE.sub(" ", s)
        s = _WHITESPACE_RE.sub(" ", s)
        return s.strip()

    v = _normalize(value)
    # Common observed formats (seconds present vs not).
    for fmt in (
        "%A, %B %d, %Y at %I:%M:%S %p",
        "%A, %B %d, %Y at %I:%M %p",
    ):
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue

    # Fallbacks (in case the script output format changes)
    try:
        return datetime.fromisoformat(v)
    except Exception as e:  # noqa: BLE001 - raise a clearer message below
        raise ValueError(f"Unrecognized mail date format: {value!r}") from e


if __name__ == "__main__":
    for msg in messages(mailbox="sent"):
        LOG.info(msg)
        # LOG.info(
        #     message(
        #         msg_meta.id, account=msg_meta.account, mailbox=msg_meta.mailbox
        #     )
        # )
    # '/Users/reggie.pierce/Library/Mail/V10/103D4CC4-6474-4EFD-810B-3655F0D3279C/[Gmail].mbox/All Mail.mbox/BC175B50-FFF1-438B-BAC9-672E07A54BFB/Data/2/Messages'
