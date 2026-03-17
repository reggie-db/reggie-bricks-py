"""Helpers for working with Databricks MLflow experiments."""

from enum import Enum
from functools import cache
from typing import Callable, Iterator

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists, ResourceDoesNotExist
from databricks.sdk.service.ml import Experiment
from lfp_logging import logs

from dbx_tools import clients

LOG = logs.logger()
_SHARED_PATH = "Shared"
_USERS_PATH = "Users"


class ExperimentPathType(Enum):
    SHARED = "shared"
    USER = "user"


def get(
    experiment_request: str,
    create_experiment_path_type: ExperimentPathType | None = ExperimentPathType.USER,
    workspace_client: WorkspaceClient | None = None,
) -> Experiment:
    """Return a Databricks experiment by id or name.

    Args:
        experiment_request: MLflow experiment id, name, or workspace path to
            look up.
        create: If True, create the experiment if it does not exist.
        workspace_client: Optional Databricks workspace client. When omitted,
            the shared default client is used.

    Returns:
        The matching Databricks ``Experiment``.
    """
    if not workspace_client:
        workspace_client = clients.workspace_client()

    @cache
    def _current_user_name() -> str:
        return workspace_client.current_user.me().user_name

    for experiment_path_type in {create_experiment_path_type, None}:
        matched_experiment: Experiment | None = None
        for request in _experiment_requests(
            experiment_request=experiment_request,
            current_user_name_fn=_current_user_name,
        ):
            if experiment := _get(
                workspace_client=workspace_client, experiment_request=request
            ):
                if matched_experiment is not None:
                    raise RuntimeError(
                        f"Multiple experiments found for request: {experiment_request}"
                    )
                matched_experiment = experiment

        if matched_experiment:
            return matched_experiment
        elif experiment_path_type:
            if experiment_request.isdigit():
                raise ValueError(
                    f"Experiment ID cannot be used as a path: {experiment_request}"
                )
            experiment_path, user_name = _experiment_path(
                experiment_request=experiment_request
            )
            if ExperimentPathType.SHARED == experiment_path_type:
                if user_name:
                    raise ValueError(
                        f"Shared experiment path type cannot have a user_name: {experiment_request}"
                    )
                experiment_path = f"/Shared/{experiment_path}"
            elif ExperimentPathType.USER == experiment_path_type:
                if not user_name:
                    user_name = _current_user_name()
                experiment_path = f"/Users/{user_name}/{experiment_path}"
            else:
                raise ValueError(
                    f"Invalid experiment path type: {experiment_path_type}"
                )
            try:
                LOG.debug(f"Creating experiment - experiment_path:%s", experiment_path)
                experiment_id = workspace_client.experiments.create_experiment(
                    experiment_path
                ).experiment_id
                return workspace_client.experiments.get_experiment(
                    experiment_id=experiment_id
                ).experiment
            except ResourceAlreadyExists:
                LOG.debug(
                    f"Error creating experiment - experiment_request:%s",
                    experiment_request,
                    exc_info=True,
                )
                continue
    raise ResourceDoesNotExist(f"Experiment not found: {experiment_request}")


def fetch(
    experiment_request: str,
    workspace_client: WorkspaceClient | None = None,
) -> Iterator[Experiment]:
    """Return Databricks experiments matching an id or name request.

    Args:
        experiment_request: MLflow experiment id, name, or workspace path to
            look up.
        workspace_client: Optional Databricks workspace client. When omitted,
            the shared default client is used.

    Returns:
        The matching Databricks ``Experiment``.

    Raises:
        RuntimeError: If Databricks returns a successful response without an
            experiment payload.
    """
    if not experiment_request:
        return []
    if not workspace_client:
        workspace_client = clients.workspace_client()
    # Normalize ids and workspace-style names into the candidate lookup paths.
    experiment_requests = _experiment_requests(
        experiment_request=experiment_request,
        current_user_name_fn=lambda: workspace_client.current_user.me().user_name,
    )
    for request in experiment_requests:
        experiment = _get(workspace_client=workspace_client, experiment_request=request)
        if experiment:
            yield experiment


def _get(
    workspace_client: WorkspaceClient, experiment_request: str
) -> Experiment | None:
    if experiment_request.isdigit():
        try:
            return workspace_client.experiments.get_experiment(
                experiment_id=experiment_request
            ).experiment
        except ResourceDoesNotExist:
            LOG.debug(
                f"Error fetching experiment - experiment_id:%s",
                experiment_request,
                exc_info=True,
            )
    else:
        try:
            return workspace_client.experiments.get_by_name(
                experiment_name=experiment_request
            ).experiment
        except ResourceDoesNotExist:
            LOG.debug(
                f"Error fetching experiment - experiment_name:%s",
                experiment_request,
                exc_info=True,
            )
    return None


def _experiment_requests(
    experiment_request: str, current_user_name_fn: Callable[[], str]
) -> Iterator[str]:
    """Expand an experiment request into the candidate ids and workspace paths."""
    if experiment_request.isdigit():
        yield experiment_request
    else:
        experiment_path, user_name = _experiment_path(
            experiment_request=experiment_request
        )
        if not user_name:
            yield f"/Users/{current_user_name_fn()}/{experiment_path}"
            yield f"/Shared/{experiment_path}"
            yield experiment_path
        else:
            yield f"/Users/{user_name}/{experiment_path}"


def _experiment_path(experiment_request: str) -> tuple[str, str | None]:
    """Normalize a request into a relative experiment path and optional username."""
    path_parts = experiment_request.split("/")
    if len(path_parts) > 1:
        if path_parts[0] == _SHARED_PATH:
            return "/".join(path_parts[1:]), None
        elif path_parts[0] == _USERS_PATH and len(path_parts) > 2:
            return "/".join(path_parts[2:]), path_parts[1]
    return experiment_request, None


if __name__ == "__main__":
    import os

    os.environ["DATABRICKS_CONFIG_PROFILE"] = "RACETRAC-DEV"
    print(get(experiment_request="store-intelligence-v3"))
