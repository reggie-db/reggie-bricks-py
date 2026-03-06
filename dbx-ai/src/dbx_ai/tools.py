import asyncio
from typing import Any, Iterable, TypeAlias

from dbx_core import objects, strs
from lfp_types import T, to_iterable
from pydantic_ai import Agent, RunContext

from dbx_ai import agents

Instructions: TypeAlias = Iterable["Instructions"] | str | None


async def summarize(
    text: str,
    instructions: Instructions = None,
    run_context: RunContext[Any] | None = None,
    agent: Agent | None = None,
) -> str:
    return await run(
        text,
        [instructions, "Summarize the text without commentary"],
        agent=agent,
        run_context=run_context,
        output_type=str,
    )


async def prompt(
    *values: Any,
    instructions: Instructions = None,
    run_context: RunContext[Any] | None = None,
    agent: Agent | None = None,
) -> str:
    if values:
        data_json = objects.to_json(values)
        if data_json:
            tool_instructions = """
            You are a prompt generator.
            
            Your task is to take a JSON representation of data and convert it into a clean,
            human-readable prompt. The resulting prompt should be structured so that another
            LLM can easily understand the information.
            
            Important:
            - Some input values may already contain text that is formatted like a prompt.
            - Preserve useful structure when it already exists.
            - Clean up inconsistent indentation or formatting if needed, but do not unnecessarily rewrite or rephrase existing prompt-style content.
            
            Rules for formatting:
            
            1. Strings
            - Output string values exactly as they appear.
            - Do not add quotes unless they already exist in the input.
            - If a string already resembles prompt-style text (headers, lists, sections), preserve that structure and only normalize indentation if necessary.
            
            2. Simple key-value pairs
            - If a key maps directly to a primitive value (string, number, boolean), render it on a single line.
            
            Format:
            key: value
            
            Example:
            name: John
            age: 32
            status: active
            
            3. Lists
            Default behavior:
            - Render lists as bullet points.
            - Each element appears on its own line.
            - Use "-" as the bullet marker.
            
            Example:
            - item one
            - item two
            - item three
            
            If the list belongs to a key, place the key on its own line with a colon and render the list below it.
            
            Example:
            items:
                - item one
                - item two
                - item three
            
            3a. Numbered lists (when detected)
            - If list elements appear to represent a numbered sequence, render them as a numbered list instead of bullets.
            - Detect numbering if:
              - Elements begin with a numeric prefix such as "1.", "2.", "3." or "1)", "2)", etc.
              - The numbers are contiguous and correctly ordered (1,2,3,...).
            - When rendering numbered lists:
              - Use "1.", "2.", "3." etc.
              - Remove the detected numeric prefix from the item text to avoid duplication.
            
            Example input:
            ["1. first step", "2. second step", "3. third step"]
            
            Output:
            1. first step
            2. second step
            3. third step
            
            If the numbered list belongs to a key:
            
            steps:
                1. first step
                2. second step
                3. third step
            
            4. Maps / Objects
            - Treat object keys as headers.
            - Headers must end with ":".
            - If the value is another object, list, or complex structure, place the key on its own line with ":".
            - Render the nested content below the header.
            
            Example:
            title:
                Some text here
            
            Additionally:
            - Treat any standalone line of text that ends with ":" (and is not a "key: value" pair) as a header.
            - Content following that header should be nested under it.
            
            5. Nesting
            - Whenever content appears under a header, indent it by four spaces.
            - A header may be:
              - an object key rendered as "key:"
              - a standalone line of text ending in ":".
            
            - Whenever content appears under a list item, indent it by four spaces relative to that list item.
            
            - Lists may be nested under headers or under other list items.
            - When a list item contains subpoints, render the subpoints as a nested list indented one level deeper.
            - Maintain consistent indentation depth throughout the prompt.
            
            Example:
            
            header:
                subheader:
                    1. first step
                    2. second step
                description: Some string value
            
            Example:
            
            Todo List:
                1. first step
                    - first step subpoint
                2. second step
            
            Example:
            
            Here are my priorities for the year:
                1. Sell more gas
                2. Improve customer satisfaction
                    - especially at pumps
                    - for now well ignore florida
                3. Expand into new markets
            
            6. Data cleanup
            Before producing the output prompt:
            
            - Remove duplicate entries.
            - Remove redundant content.
            - Remove invalid or empty values:
              - null
              - empty strings
              - empty lists
              - empty objects
            - Preserve only meaningful information.
            
            7. Output requirements
            - Produce only the final prompt text.
            - Do not explain your reasoning.
            - Do not output JSON.
            - Do not add commentary.
            
            Input JSON will be provided below.
            Transform it into the formatted prompt following all rules above.
            """
            return await run(
                data_json,
                [instructions, tool_instructions],
                agent=agent,
                run_context=run_context,
                output_type=str,
            )
    return ""


async def run(
    user_prompt: str,
    instructions: Instructions = None,
    output_type: type[T] | None = None,
    run_context: RunContext[Any] | None = None,
    agent: Agent | None = None,
    **kwargs,
) -> T | None:
    run_prompt = _run_prompt(user_prompt, instructions)
    if not run_prompt:
        return None
    if agent is None:
        agent = agents.small()
    usage = run_context.usage if run_context else None
    result = await agent.run(run_prompt, usage=usage, output_type=output_type, **kwargs)
    if result:
        output = result.output
        if isinstance(output, str):
            output = strs.trim(output)
        return output
    return "" if output_type is str else None


def _run_prompt(user_prompt: str, instructions: Instructions) -> str:
    run_prompt = []
    for instruction in to_iterable(instructions, flatten=True):
        instruction = strs.trim(instruction)
        if instruction:
            if not run_prompt:
                run_prompt.append("Instructions:\n")
            run_prompt.append(instruction)
    user_prompt = strs.trim(user_prompt)
    if user_prompt:
        if run_prompt:
            run_prompt.append("\nUser Prompt:\n")
        run_prompt.append(user_prompt)
    return "\n".join(run_prompt)


async def main():
    blob = """
    Here are my priorities for the year:
     1. Sell more gas
         2. Improve customer satisfaction
              - especially at pumps
              - for now well ignore florida
       3. Expand into new markets
     
 
    """
    data: dict = {
        "task": "Prepare a store operations summary for leadership based on the following information.",
        "context": {
            "company": "RaceTrac",
            "region": "Southeast",
            "date": "2026-03-04",
            "audience": [
                "Store operators",
                "Regional managers",
                "Executive leadership",
            ],
        },
        "stores": [
            {
                "store_id": "RT2312",
                "location": {"city": "Atlanta", "state": "GA"},
                "metrics": {
                    "sales": 125430,
                    "shrink_percent": 1.3,
                    "inventory_turnover": 6.2,
                    "oos_rate": 0.04,
                },
                "alerts": [
                    "Hot bar inventory running low",
                    "Unusual waste detected in pizza prep",
                    "Hot bar inventory running low",
                ],
                "recommendations": [
                    "Increase dough prep by 10%",
                    "Audit waste process during evening      shift",
                    "Increase dough prep by 10%%",
                ],
            },
            {
                "store_id": "RT2604",
                "location": {"city": "Orlando", "state": "FL"},
                "metrics": {
                    "sales": 98210,
                    "shrink_percent": 2.1,
                    "inventory_turnover": 5.7,
                    "oos_rate": 0.09,
                },
                "alerts": [
                    "Frequent stockouts on breakfast sandwiches",
                    "Labor allocation misaligned with peak demand",
                    None,
                ],
                "recommendations": [
                    "Increase morning prep staff",
                    "Adjust inventory reorder thresholds",
                    "",
                ],
            },
        ],
        "initiatives": {
            "forecasting_upgrade": {
                "description": "Improve demand forecasting using ML-based models.",
                "objectives": [
                    "Reduce food waste",
                    "Improve product availability",
                    "Optimize labor scheduling",
                ],
                "data_sources": [
                    "POS transactions",
                    "Camera-based hot bar monitoring",
                    "Weather data",
                    "POS transactions",
                ],
            },
            "technology_stack": {
                "platform": "Databricks",
                "components": [
                    "Lakeflow Connect",
                    "Delta Live Tables",
                    "Lakebase",
                    "Databricks Apps",
                ],
            },
        },
        "notes": [
            "Executive summary should prioritize operational impact.",
            "Focus on actionable insights rather than raw metrics.",
            "Remove duplicate information in the final prompt.",
        ],
    }
    result = await prompt(None, blob, data)
    print(result)
    print(await summarize(None, result, instructions=["no more than 3 sentances"]))


if __name__ == "__main__":
    asyncio.run(main())
