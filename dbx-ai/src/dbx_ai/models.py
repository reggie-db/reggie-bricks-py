import functools
import os
from typing import Iterable

from databricks_tools_core import get_workspace_client
from dbx_tools import configs
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
            endpoint = get_workspace_client().serving_endpoints.get(model_name)
            # Only choose endpoints that are serving traffic.
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
        # Prioritize higher-quality reasoning models first.
        "databricks-claude-sonnet-4-6",
        "databricks-gpt-5-2",
        "databricks-gemini-2-5-pro",
        "databricks-claude-sonnet-4-5",
        "databricks-llama-4-maverick",
    ]
    return _model("REASONING_MODEL", models)


def memory() -> str:
    """
    Target: Mem0 (Summarization & Fact Extraction)
    Requirements: High throughput, low cost, strict JSON adherence.
    """
    models = [
        # Mem0 write paths benefit from fast, stable chat models.
        "databricks-gpt-5-mini",
        "databricks-gemini-2-5-flash",
        "databricks-claude-haiku-4-5",
        "databricks-meta-llama-3-3-70b-instruct",
    ]
    return _model("MEMORY_MODEL", models)


def vectorize() -> str:
    """
    Target: pgvector (Embeddings)
    Requirements: Consistent dimensions (essential for pgvector stability).
    """
    models = [
        # Prefer production-ready Databricks embedding endpoints in this workspace.
        "databricks-gte-large-en",
        "databricks-bge-large-en",
        "databricks-qwen3-embedding-0-6b",
    ]
    return _model("VECTOR_MODEL", models)


def ranker() -> str:
    """
    Target: Reranking Mem0 results for precision.
    """
    models = [
        # Mem0 supports LLM rerankers; use fast chat endpoints when
        # dedicated reranker endpoints are unavailable in workspace.
        "databricks-gpt-5-mini",
        "databricks-gemini-2-5-flash",
        "databricks-claude-haiku-4-5",
    ]
    return _model("RANKER_MODEL", models)


def _model(config_key: str, models: list[str]) -> str:
    return _model_cache(config_key, ",".join(models))


def _configured_model(config_key: str) -> str | None:
    """Read model override from env first, then dbx_tools config."""
    env_value = os.getenv(config_key)
    if env_value:
        return env_value
    try:
        return configs.value(config_key, None)
    except Exception:
        LOG.debug("No config override found for key=%s", config_key, exc_info=True)
        return None


@functools.cache
def _model_cache(config_key: str, model_csv: str) -> str:
    model_name = _configured_model(config_key)
    if not model_name:
        model_name = model(model_csv.split(","))
    if not model_name:
        raise ValueError(f"No model found - config_key:{config_key} models:{model_csv}")
    return model_name


if __name__ == "__main__":
    LOG.info("reasoning model: %s", reasoning())
    LOG.info("memory model: %s", memory())
    LOG.info("vector model: %s", vectorize())
    LOG.info("ranker model: %s", ranker())
