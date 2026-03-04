from typing import Union

from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext

from dbx_ai import agents, models


# 1. Define the flexible response model
class SummarizedContent(BaseModel):
    """
    A flexible container for summaries.
    Can be a single string, a list of strings, or nested lists.
    """

    content: str | list[Union[str, "SummarizedContent"]] = Field(
        ..., description="The summarized data, potentially nested."
    )


# 2. Create the Agent
# We define the dependency type if needed, but for a direct LLM call,
# the RunContext is available by default.
agent = agents.create()

_summarizer_agent = Agent(
    agents.model(models.summarize()), output_type=SummarizedContent
)


# 3. Define the Tool using Agent Delegation
@agent.tool
async def summarize_text(ctx: RunContext[None], text: str) -> SummarizedContent:
    """
    Summarizes unstructured text into a structured format.
    Delegates the task to a sub-agent using the SAME model instance.
    """
    print(f"--- Tool triggered: Delegating summary of {len(text)} chars ---")

    # Create a 'one-off' agent using the model from the current context
    # This inherits the provider and configuration (e.g., API keys)

    # Run the sub-agent.
    # Pass ctx.usage so the tokens count towards the main run's limits/tracking.
    result = await _summarizer_agent.run(text, usage=ctx.usage)

    return result.output


# 4. Example Execution
if __name__ == "__main__":
    import asyncio

    async def main():
        user_input = (
            "The history of AI spans decades. Early work focused on symbolic logic. "
            "Later, connectionism and neural networks gained traction. "
            "Today, LLMs dominate the landscape with transformer architectures."
        )

        result = await agent.run(
            f"Please summarize this history: {user_input}", output_type=dict
        )

        print("\nFinal Structured Output:")
        print(result.output)

    asyncio.run(main())
