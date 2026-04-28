"""Interactive DuckDuckGo search via ``ddgs`` and a PydanticAI ``Agent`` tool (streamed replies).

Manual script (not pytest). Launches an interactive chat by default. From the repo root::

    uv run --project dbx-ai --group dev python dbx-ai/tests/duckduckgo_pydantic_demo.py

Single query then exit::

    uv run --project dbx-ai --group dev python dbx-ai/tests/duckduckgo_pydantic_demo.py -q \"What is the Fed rate?\"
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from ddgs import DDGS
from lfp_logging import logs
from pydantic import BaseModel
from pydantic_ai import Agent, RunContext

from dbx_ai import agents

LOG = logs.logger()

_DDG_SEARCH_DELAY_SEC = 1.0
_INTERACTIVE_EXIT = frozenset({"", "exit", "quit", ":q"})


class Result(BaseModel):
    title: str
    body: str
    href: str


def _build_agent() -> Agent[None, str]:
    """Return an agent with ``search_tool`` calling live ``web_search``."""
    agent = agents.create(
        instructions="Format results in markdown, with markdown links where needed."
    )

    @agent.tool
    def web_search(ctx: RunContext[None], query: str) -> list[Result]:
        """Searches the web using DuckDuckGo to find current events or data.

        Returns a bullet list string for the model to read, or a short message when
        there are no hits.
        """
        with DDGS() as ddgs:
            results = ddgs.text(query, max_results=5)
        return [Result.model_validate(r) for r in results]

    return agent


def _configure_logging(verbose: bool, *, interactive: bool) -> None:
    """Configure root logging; keep stdout clean for streamed agent text unless verbose."""
    if interactive and not verbose:
        level = logging.WARNING
    else:
        level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Interactive DuckDuckGo + PydanticAI chat (TestModel; streamed). "
            "By default reads stdin until EOF or an empty line."
        ),
    )
    parser.add_argument(
        "--query",
        "-q",
        default=None,
        metavar="TEXT",
        help="Run one prompt and exit instead of interactive mode.",
    )
    parser.add_argument(
        "--direct",
        action="store_true",
        help="With -q only: run DuckDuckGo text search and print results (no agent).",
    )
    parser.add_argument(
        "--no-delay",
        action="store_true",
        help="Skip the pause before each DuckDuckGo request (not recommended for rapid fire).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args(argv)


async def _stream_agent_reply(agent: Agent[None, str], user_prompt: str) -> None:
    """Run the agent with ``run_stream`` and write streamed text to stdout."""
    async with agent.run_stream(user_prompt) as response:
        async for chunk in response.stream_text(delta=True):
            print(chunk, end="", flush=True)
    sys.stdout.write("\n")
    sys.stdout.flush()


async def _readline_interactive() -> str | None:
    """Prompt on stderr and read one line from stdin (EOF returns None)."""

    def _read() -> str:
        print("You> ", end="", flush=True, file=sys.stderr)
        return sys.stdin.readline()

    line = await asyncio.to_thread(_read)
    if line == "":
        return None
    return line.rstrip("\n\r")


async def _maybe_search_delay(no_delay: bool) -> None:
    if no_delay:
        return
    await asyncio.sleep(_DDG_SEARCH_DELAY_SEC)


async def _run_interactive_chat(agent: Agent[None, str], *, no_delay: bool) -> None:
    """Read lines from stdin; stream agent output for each non-empty message."""
    print(
        "DuckDuckGo + PydanticAI chat. Type a question, Enter to send. "
        "Empty line, exit, quit, or Ctrl-D to stop.\n",
        file=sys.stderr,
    )
    while True:
        line = await _readline_interactive()
        if line is None:
            break
        key = line.strip().lower()
        if key in _INTERACTIVE_EXIT:
            break

        await _maybe_search_delay(no_delay)
        await _stream_agent_reply(agent, line)


async def _async_main(args: argparse.Namespace) -> int:
    """Async entry: interactive chat or single streamed run."""

    _configure_logging(args.verbose, interactive=True)
    agent = _build_agent()
    await _run_interactive_chat(agent, no_delay=args.no_delay)
    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse CLI args and run interactive chat or a single query."""
    args = _parse_args(argv)
    interactive = args.query is None
    if not interactive:
        _configure_logging(args.verbose, interactive=False)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    # raise SystemExit(main(sys.argv[1:]))
    import json

    domains = [
        "convenience.org",
        "csnews.com",
        "cstoredecisions.com",
        "opis.com",
        "natso.com",
    ]

    site_query = "(" + " OR ".join(f"site:{d}" for d in domains) + ")"
    with DDGS() as ddgs:
        results = ddgs.text(f"racetrac {site_query}", max_results=25)
        for result in results:
            print(result["title"])
            print(result["body"])
            print(result["href"])
            print(json.dumps(result))
            print("-" * 100)
