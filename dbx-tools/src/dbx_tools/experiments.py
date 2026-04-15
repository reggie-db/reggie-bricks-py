"""Helpers for resolving and creating Databricks MLflow experiments.

This module accepts experiment lookups as either an experiment id, a plain
experiment name, or a Databricks workspace path such as ``/Shared/foo`` or
``/Users/user@example.com/foo``.

Plain names are expanded into the most common Databricks workspace locations so
callers do not need to know the exact stored experiment path ahead of time.
"""

from enum import Enum
from typing import Iterator

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists, ResourceDoesNotExist
from databricks.sdk.service.ml import (
    CreateExperimentResponse,
    Experiment,
    ExperimentAccessControlRequest,
    ExperimentPermissionLevel,
)
from dbx_ai.agents import projects, strs
from lfp_logging import logs

from dbx_tools import clients, runtimes

LOG = logs.logger()
_SHARED_PATH = "/Shared/"
_USERS_PATH = "/Users/"


class ExperimentPathType(Enum):
    """Workspace path types supported when creating missing experiments."""

    SHARED = "shared"
    USER = "user"


class _ExperimentLookupContext:
    """Lazy lookup context shared across experiment resolution helpers.

    The context caches the resolved workspace client and current user name so
    repeated candidate lookups do not need to make duplicate SDK calls.
    """

    def __init__(self, workspace_client: WorkspaceClient | None = None):
        self._workspace_client = workspace_client
        self._current_user_name = None

    @property
    def workspace_client(self) -> WorkspaceClient:
        """Return the resolved workspace client for experiment operations."""
        if self._workspace_client is None:
            self._workspace_client = clients.workspace_client()
        return self._workspace_client

    @property
    def current_user_name(self) -> str:
        """Return the current Databricks user name, caching the first lookup."""
        if self._current_user_name is None:
            self._current_user_name = (
                self.workspace_client.current_user.me().user_name or ""
            )
        return self._current_user_name


def get(
    experiment_lookup: str | None = None,
    create_experiment_path_type: ExperimentPathType | None = ExperimentPathType.USER,
    workspace_client: WorkspaceClient | None = None,
) -> Experiment:
    """Return a Databricks experiment for an id, name, or workspace path.

    Args:
        experiment_lookup: MLflow experiment id, name, or workspace path to
            look up.
        create_experiment_path_type: Optional path type to use when creating a
            missing experiment after lookup fails. When set to
            ``ExperimentPathType.USER`` or ``ExperimentPathType.SHARED``, the
            request is converted into the corresponding workspace path. When
            ``None``, missing experiments are not created.
        workspace_client: Optional Databricks workspace client. When omitted,
            the shared default client is used.

    Returns:
        The matching Databricks ``Experiment``.

    Raises:
        ValueError: If an experiment id is used where a creatable path is
            required, or if the requested create path type conflicts with the
            request format.
        ResourceDoesNotExist: If no experiment can be found and creation is
            disabled or unsuccessful.
        RuntimeError: If more than one candidate path resolves to an existing
            experiment.
    """
    if not experiment_lookup and create_experiment_path_type is not None:
        root_project_name = projects.root_project_name()
        if root_project_name:
            if root_project_name := "-".join(strs.tokenize(root_project_name)):
                experiment_lookup = root_project_name
    if not experiment_lookup:
        raise ValueError(f"Experiment lookup is required: {experiment_lookup}")
    lookup_ctx = _ExperimentLookupContext(workspace_client)
    result_experiment: Experiment | None = None
    for experiment_path_type in {create_experiment_path_type, None}:
        for lookup in _experiment_lookups(lookup_ctx, experiment_lookup):
            if experiment := _get(lookup_ctx, lookup):
                if result_experiment is not None:
                    raise RuntimeError(
                        f"Multiple experiments found for lookup: {experiment_lookup}"
                    )
                result_experiment = experiment

        if result_experiment:
            break
        elif experiment_path_type:
            create_experiment_response: CreateExperimentResponse | None = None
            try:
                create_experiment_response = (
                    lookup_ctx.workspace_client.experiments.create_experiment(
                        _experiment_path(
                            lookup_ctx, experiment_lookup, experiment_path_type
                        )
                    )
                )
            except ResourceAlreadyExists:
                LOG.debug(
                    "Experiment exists: %s",
                    experiment_lookup,
                    exc_info=True,
                )
                continue
            if create_experiment_response and create_experiment_response.experiment_id:
                result_experiment = lookup_ctx.workspace_client.experiments.get_experiment(
                    experiment_id=create_experiment_response.experiment_id
                ).experiment
                break

    if result_experiment is None or not result_experiment.experiment_id:
        raise ResourceDoesNotExist(f"Experiment not found: {experiment_lookup}")

    _grant_app_creator_permissions(lookup_ctx, result_experiment.experiment_id)
        
    return result_experiment


def fetch(
    experiment_lookup: str,
    workspace_client: WorkspaceClient | None = None,
) -> Iterator[Experiment]:
    """Yield experiments that match an experiment id, name, or workspace path.

    Args:
        experiment_lookup: MLflow experiment id, name, or workspace path to
            look up.
        workspace_client: Optional Databricks workspace client. When omitted,
            the shared default client is used.

    Yields:
        Each matching Databricks ``Experiment`` found from the normalized
        lookup candidates.

    Notes:
        Plain experiment names are expanded into user and shared workspace
        paths before the bare name is attempted.
    """
    lookup_ctx = _ExperimentLookupContext(workspace_client)
    for lookup in _experiment_lookups(lookup_ctx, experiment_lookup):
        if experiment := _get(lookup_ctx, lookup):
            yield experiment


def _experiment_path(
    lookup_ctx: _ExperimentLookupContext,
    experiment_lookup: str,
    experiment_path_type: ExperimentPathType | None = None,
) -> str:
    """Return a creatable workspace path for an experiment lookup.

    Args:
        lookup_ctx: Shared lookup context used to resolve the current user name.
        experiment_lookup: Experiment id, plain name, or workspace path.
        experiment_path_type: Target path type to enforce for creation.

    Returns:
        A Databricks workspace path suitable for experiment creation.

    Raises:
        ValueError: If the requested path type conflicts with the incoming
            lookup format.
    """
    if experiment_path_type:
        if experiment_path_type == ExperimentPathType.SHARED:
            if experiment_lookup.startswith(_USERS_PATH):
                raise ValueError(
                    f"User experiment path type cannot be used for shared experiments: {experiment_lookup}"
                )
            elif not experiment_lookup.startswith(_SHARED_PATH):
                return f"/Shared/{experiment_lookup}"
        elif experiment_path_type == ExperimentPathType.USER:
            if experiment_lookup.startswith(_SHARED_PATH):
                raise ValueError(
                    f"Shared experiment path type cannot be used for user experiments: {experiment_lookup}"
                )
            elif not experiment_lookup.startswith(_USERS_PATH):
                return f"/Users/{lookup_ctx.current_user_name}/{experiment_lookup}"
    return experiment_lookup


def _experiment_lookups(
    lookup_ctx: _ExperimentLookupContext, experiment_lookup: str
) -> Iterator[str]:
    """Expand an experiment lookup into candidate ids and workspace paths.

    The original lookup is always yielded first. Bare experiment names are then
    expanded into the current user's workspace path and the shared workspace
    path. Fully qualified ``/Users/...`` and ``/Shared/...`` lookups are left
    unchanged.
    """
    if experiment_lookup:
        yield experiment_lookup
        if not experiment_lookup.startswith(
            _USERS_PATH
        ) and not experiment_lookup.startswith(_SHARED_PATH):
            if current_user_name := lookup_ctx.current_user_name:
                yield f"/Users/{current_user_name}/{experiment_lookup}"
            yield f"/Shared/{experiment_lookup}"


def _grant_app_creator_permissions(
    lookup_ctx: _ExperimentLookupContext, experiment_id: str
) -> None:
    """Grant CAN_MANAGE to the app creator when running inside a Databricks App.

    When the process is detected as a Databricks App via ``runtimes.app_info()``,
    the app's creator is fetched and added to the experiment's permission list so
    they retain access even when the app runs under a service principal identity.
    Fetches the current ACL, merges in the creator entry, then resets all permissions.
    No-ops silently if the creator already has CAN_MANAGE.

    Args:
        lookup_ctx: Shared lookup context used to reach the workspace client.
        experiment_id: The experiment id to grant permissions on.
    """
    app = runtimes.app_info()
    if not app:
        return
    try:
        creator = lookup_ctx.workspace_client.apps.get(app.name).creator
    except Exception:
        LOG.warning("Could not retrieve app creator for %s", app.name, exc_info=True)
        return
    if not creator:
        return
    try:
        current = lookup_ctx.workspace_client.experiments.get_permissions(
            experiment_id=experiment_id
        )
        existing = [
            ExperimentAccessControlRequest(
                user_name=acl.user_name,
                group_name=acl.group_name,
                service_principal_name=acl.service_principal_name,
                permission_level=acl.all_permissions[0].permission_level
                if acl.all_permissions
                else None,
            )
            for acl in (current.access_control_list or [])
            if acl.user_name != creator
        ]
        creator_acl = next(
            (acl for acl in (current.access_control_list or []) if acl.user_name == creator),
            None,
        )
        if creator_acl and any(
            p.permission_level == ExperimentPermissionLevel.CAN_MANAGE
            for p in (creator_acl.all_permissions or [])
        ):
            return
        lookup_ctx.workspace_client.experiments.set_permissions(
            experiment_id=experiment_id,
            access_control_list=existing
            + [
                ExperimentAccessControlRequest(
                    user_name=creator,
                    permission_level=ExperimentPermissionLevel.CAN_MANAGE,
                )
            ],
        )
        LOG.info(
            "Granted CAN_MANAGE on experiment %s to app creator %s",
            experiment_id,
            creator,
        )
    except Exception:
        LOG.warning(
            "Failed to grant CAN_MANAGE on experiment %s to app creator %s",
            experiment_id,
            creator,
            exc_info=True,
        )


def _get(
    lookup_ctx: _ExperimentLookupContext, experiment_lookup: str
) -> Experiment | None:
    """Resolve a single experiment lookup candidate to an experiment.

    Numeric values are attempted as experiment ids first. All values are then
    attempted as exact Databricks experiment names.
    """
    if experiment_lookup.isdigit():
        try:
            return lookup_ctx.workspace_client.experiments.get_experiment(
                experiment_id=experiment_lookup
            ).experiment
        except ResourceDoesNotExist:
            LOG.debug(
                "Experiment ID does not exist:%s",
                experiment_lookup,
                exc_info=True,
            )
    try:
        return lookup_ctx.workspace_client.experiments.get_by_name(
            experiment_name=experiment_lookup
        ).experiment
    except ResourceDoesNotExist:
        LOG.debug(
            "Experiment name does not exist:%s",
            experiment_lookup,
            exc_info=True,
        )
    return None


if __name__ == "__main__":
    import os

    os.environ["DATABRICKS_CONFIG_PROFILE"] = "RACETRAC-DEV"
    print(get(experiment_lookup="store-intelligence-v3"))
