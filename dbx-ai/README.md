# dbx-ai

PydanticAI helpers for Databricks model serving.

## What It Provides

- `agents.py`
  - Creates configured `pydantic_ai.Agent` instances.
  - Builds OpenAI-compatible clients backed by Databricks serving endpoints.
  - Supports optional MLflow autologging setup.
- `tools.py`
  - Reusable prompt helpers such as `run`, `prompt`, and `summarize`.
- `models.py`
  - Model name helpers used by agent factories.

## Usage

```python
from dbx_ai import agents, tools

agent = agents.small()
result = await tools.run("Summarize this text.", agent=agent, output_type=str)
```
