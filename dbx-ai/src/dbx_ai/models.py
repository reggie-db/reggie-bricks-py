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


def summarize() -> str:
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
    """
    Finds a model by searching for it in a comma-separated list of models, or
    by looking up a configuration key.

    :param config_key: The configuration key to look up.
    :param models: A list of model names to search for if the configuration key is not found.
    :return: The name of the first available model found.
    """
    return _model_cache(config_key, ",".join(models))


@functools.cache
def _model_cache(config_key: str, model_csv: str) -> str:
    """
    A cached version of the model finding logic.

    It first checks the configuration for a value associated with `config_key`.
    If no value is found, it attempts to find the first 'ready' model among the
    models listed in `model_csv`.

    :param config_key: The configuration key used for lookup.
    :param model_csv: A comma-separated string of model names to fallback on.
    :return: The name of the selected model.
    :raises ValueError: If no model is found in the configuration or the provided CSV.
    """
    model_name = configs.value(config_key, None)
    if not model_name and model_csv:
        model_name = model((m.strip() for m in model_csv.split(",")))
    if not model_name:
        raise ValueError(f"No model found - config_key:{config_key} models:{model_csv}")
    return model_name


if __name__ == "__main__":
    print("starting")
    LOG.info("reasoning model: %s", reasoning())
    LOG.info("memory model: %s", summarize())
    LOG.info("vector model: %s", vectorize())
    LOG.info("ranker model: %s", ranker())
