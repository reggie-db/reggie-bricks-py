import asyncio
from datetime import datetime
from itertools import islice

from lfp_logging import logs
from pydantic_ai import RunContext

from dbx_ai import dbx_agent, mail, memory

LOG = logs.logger()
agent = dbx_agent.agent()


@agent.tool
def _email_messages(
    ctx: RunContext,
    max_date: datetime | None = None,
    min_date: datetime | None = None,
    max_messages: int | None = 100,
) -> list[mail.EmailMessageMetadata]:
    """Search and filter email messages by a specific date range.

    Args:
        ctx: The runtime context.
        max_date: The NEWEST (latest) date to include. Messages newer than this are skipped.
        min_date: The OLDEST (earliest) date to include. The search stops when it hits messages older than this.
        max_messages: The maximum number of messages to return. Defaults to 100.
    """

    if max_date and max_date.tzinfo:
        max_date = max_date.replace(tzinfo=None)
    if min_date and min_date.tzinfo:
        min_date = min_date.replace(tzinfo=None)

    msg_metas = []

    for msg_meta in mail.messages():
        if max_date and msg_meta.received > max_date:
            continue

        if min_date and msg_meta.received < min_date:
            break

        msg_metas.append(msg_meta)

        if max_messages and len(msg_metas) >= max_messages:
            break

    return msg_metas


@agent.tool
def _email_message(
    ctx: RunContext, msg_meta: mail.EmailMessageMetadata
) -> mail.EmailMessage | None:
    """
    Fetch a single email message given its metadata.

    Intended for tool calling:
    - Input is a `mail.EmailMessageMetadata` object previously returned by `_email_messages`.
    - Output is a `mail.EmailMessage` with:
      - `message_id`: only the RFC 5322 "Message-ID" header value (no other headers).
      - `body`: message text content, preferring plain text, then HTML. Attachments are not included.

    Args:
        ctx: Runtime context (unused).
        msg_meta: Message metadata that must include `id`. `account` and `mailbox` may be present,
            but this tool currently uses the default account and mailbox unless `mail.message(...)`
            is updated to accept them.

    Returns:
        The parsed email message. If the underlying fetch fails, an exception may be raised.
    """
    message = mail.message(
        msg_meta.id, account=msg_meta.account, mailbox_name=msg_meta.mailbox
    )

    return message


@agent.tool
def _email_tone_messages(
    ctx: RunContext, max_messages: int | None = 10
) -> list[mail.EmailMessage]:
    """
    Return recent sent messages to learn the user's writing tone.

    Intended for tool calling:
    - Use this tool when you need examples of how the user typically writes emails.
    - The returned messages are suitable for tone/style extraction (greeting style, formality, brevity,
      sign-offs, common phrases).
    - Each returned item is a `mail.EmailMessage` containing:
      - `message_id`: only the "Message-ID" header value (no other headers).
      - `body`: message content with plain text preferred, then HTML. Attachments are not included.

    Examples (how to use the output):
    - Identify common openings and closings and mimic them in a draft reply.
    - Infer whether the user prefers short bullet points vs prose.
    - Detect if the user tends to be formal, direct, or casual.

    Args:
        ctx: Runtime context (unused).
        max_messages: Maximum number of recent sent messages to return. Defaults to 10.

    Returns:
        A list of recent sent `mail.EmailMessage` objects.
    """
    sent_msg_metas = islice(mail.messages(mailbox_name="sent"), max_messages)

    msgs = [
        mail.message(
            msg_meta.id, account=msg_meta.account, mailbox_name=msg_meta.mailbox
        )
        for msg_meta in sent_msg_metas
    ]

    return msgs


@agent.tool
def _memory_recall(
    ctx: RunContext, type: str, question: str, limit: int = 5
) -> list[str]:
    """
    Recall cached answers for a question within a given context.

    Use this as a cache for repeated questions and answers. If you have already solved a similar
    question earlier in this session or workflow, call `_memory_recall` before re-computing the
    answer.

    The `type` parameter is the namespace for the cache. It should describe the context you are
    caching for, for example:
    - "email"
    - "tone"
    - "sql"
    - "databricks"

    Args:
        ctx: Runtime context (unused).
        type: Cache namespace/context.
        question: The question you want to look up.
        limit: Maximum number of cached entries to return.

    Returns:
        A list of cached answers (best matches first). An empty list means no cache hit.
    """
    return memory.recall(type, question, limit)


@agent.tool
def _memory_write(ctx: RunContext, type: str, question: str, answer: str):
    """
    Write a question and answer pair into a contextual cache.

    Use this after you produce a useful answer so future runs can reuse it. Pair it with
    `_memory_recall` to avoid repeated work.

    The `type` parameter is the namespace for the cache. It should describe the context you are
    caching for, for example:
    - "email"
    - "tone"
    - "sql"
    - "databricks"

    Args:
        ctx: Runtime context (unused).
        type: Cache namespace/context.
        question: The question that was answered.
        answer: The answer you want to cache.
    """
    memory.write(type, question, answer)


@agent.tool
def _favorite_name(ctx: RunContext):
    """Returns the agent's favorite name."""
    return "Reggie"


@agent.tool
def _print_message(ctx: RunContext, message: str):
    """Receives a message and prints it."""
    print(f"** {message} **ad")


async def main():
    while True:
        question = (await asyncio.to_thread(input, "> ")).strip()
        if not question:
            break

        async with agent.run_stream(question) as result:
            async for text in result.stream_output(debounce_by=0.01):
                # text here is a `str` and the frontend wants
                # JSON encoded ModelResponse, so we create one
                print(text)


if __name__ == "__main__":
    asyncio.run(main())
