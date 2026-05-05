import asyncio
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieMessage
from databricks.sdk.service.sql import StatementResponse
from dbx_core import strs
from dbx_tools import clients, genie, warehouses
from lfp_logging import logs
from pydantic_ai import AgentRunResult, RunContext, Tool

from dbx_ai import agents

LOG = logs.logger()


@dataclass
class Deps:
    workspace_client: WorkspaceClient
    genie_conversation: genie.GenieConversation


@dataclass
class GenieToolResponse:
    message: GenieMessage | None
    statement_responses: list[StatementResponse]


async def _genie_tool(ctx: RunContext[Deps], message: str) -> GenieToolResponse:
    _, responses = await ctx.deps.genie_conversation.chat(message)
    statement_ids: set[str] = set()
    last_message: GenieMessage | None = None

    async for response in responses:
        for attachment in response.query_attachments():
            if attachment.statement_id:
                statement_ids.add(attachment.statement_id)
        last_message = response.message
    statement_tasks: list[asyncio.Task[StatementResponse]] = [
        asyncio.create_task(
            warehouses.statement_response_async(
                statement_id, client=ctx.deps.workspace_client
            )
        )
        for statement_id in statement_ids
    ]

    results = await asyncio.gather(*statement_tasks, return_exceptions=True)

    statement_responses: list[StatementResponse] = []

    for statement_id, result in zip(statement_ids, results):
        if isinstance(result, Exception):
            LOG.warning(
                "Failed to fetch statement response for %s: %s",
                statement_id,
                result,
            )

            continue

        statement_responses.append(result)

    return GenieToolResponse(
        message=last_message,
        statement_responses=statement_responses,
    )


async def main():
    workspace_client = clients.workspace_client()
    genie_space = genie.space(
        "FuelOpt Planner Curated",
        workspace_client=workspace_client,
        include_serialized_space=True,
    )
    description = (
        (genie_space.description or "")
        + "\n\n[GENIE METADATA]:\n"
        + (genie_space.serialized_space or "")
    )
    genie_tool = Tool(
        _genie_tool,
        name="-".join(strs.tokenize(genie_space.title)),
        description=description,
    )
    genie_conversation = genie.GenieConversation(
        space_id=genie_space.space_id,
        client=workspace_client,
    )
    deps = Deps(
        workspace_client=workspace_client, genie_conversation=genie_conversation
    )

    agent = agents.create(tools=[genie_tool])
    last_response: AgentRunResult | None = None
    while True:
        user_prompt = input("Enter a prompt: ")
        if not user_prompt:
            break
        response = await agent.run(
            deps=deps,
            user_prompt=user_prompt,
            message_history=last_response.all_messages() if last_response else [],
        )
        last_response = response
        response.run_id
        print(getattr(response, "conversation_id", None))
        print(response)


if __name__ == "__main__":
    asyncio.run(main())
