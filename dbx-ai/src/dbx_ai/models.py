import functools
from typing import Iterable

from dbx_tools import clients, configs
from lfp_logging import logs

LOG = logs.logger()


def model(models: Iterable[str]) -> str | None:
    """
    Finds the first model in the priority list that is:
    1. Registered as an endpoint in your Databricks workspace.
    2. Currently in a 'READY' state.
    """
    for model_name in models:
        if not model_name:
            continue
        try:
            endpoint = clients.workspace_client().serving_endpoints.get(model_name)
            # Ensure the model isn't currently mid-update or failed
            if endpoint.state and endpoint.state.ready:
                ready_state = endpoint.state.ready.value
                if str(ready_state).lower() == "ready":
                    return model_name
        except Exception:
            LOG.debug(f"Model not found - model_name:%s", model_name, exc_info=True)
            continue

    return None


## --- Model Categories & Selection Logic ---


def reasoning() -> str:
    """
    Target: PydanticAI (General Chat & Complex Logic)
    Requirements: High tool-calling accuracy and large context.
    """
    models = [
        "databricks-claude-sonnet-4-6",
        "databricks-gemini-3-1-pro",  # 2026 Leader in reasoning/multimodal
        "databricks-gpt-5-2",  # OpenAI flagship available via Databricks
        "databricks-claude-sonnet-4-5",
        "databricks-llama-4-maverick",  # Top-tier open-weight reasoning fallback
    ]
    return _model("REASONING_MODEL", models)


def memory() -> str:
    """
    Target: Mem0 (Summarization & Fact Extraction)
    Requirements: High throughput, low cost, strict JSON adherence.
    """
    models = [
        "databricks-gpt-5-mini",  # Extremely fast, highly reliable structured output
        "databricks-gemini-3-flash",  # Low latency for background tasks
        "databricks-meta-llama-3-3-70b-instruct",
        "databricks-mixtral-8x22b-instruct",
    ]
    return _model("MEMORY_MODEL", models)


def vectorize() -> str:
    """
    Target: pgvector (Embeddings)
    Requirements: Consistent dimensions (essential for pgvector stability).
    """
    models = [
        "databricks-qwen3-embedding-8b",  # Latest high-dim accuracy (2026)
        "databricks-openai-text-embedding-3-large",
        "databricks-bge-large-en-v1-5",
        "databricks-gte-large-en",
    ]
    return _model("VECTOR_MODEL", models)


def ranker() -> str:
    """
    Target: Reranking Mem0 results for precision.
    """
    models = ["databricks-bge-reranker-v2-m3", "databricks-gte-reranker"]
    return _model("RANKER_MODEL", models)


def _model(config_key: str, models: list[str]) -> str:
    return _model_cache(config_key, ",".join(models))


@functools.cache
def _model_cache(config_key: str, model_csv: str) -> str:
    model_name = configs.value(config_key, None)
    if not model_name:
        model_name = model(model_csv.split(","))
    if not model_name:
        raise ValueError(f"No model found - config_key:{config_key} models:{model_csv}")
    return model_name


if __name__ == "__main__":
    print(reasoning())
    print(memory())
    print(vectorize())
    print(ranker())
