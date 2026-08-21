"""Dynamic provider for Seqera compute environments."""

from datetime import datetime, timezone
from http import HTTPMethod
from typing import Any, Optional

import pulumi
from pulumi.dynamic import (
    CreateResult,
    DiffResult,
    Resource,
    ResourceProvider,
    UpdateResult,
)

from cpg_infra.driver.dynamic_providers.seqera.inputs.compute_environment import (
    ComputeEnvArgs,
    GoogleBatchConfig,
)
from cpg_infra.driver.dynamic_providers.seqera.util.api_util import (
    SeqeraApiClient,
)

_MAX_CE_NAME_LENGTH = 100


def _create_compute_env(workspace_id: int, ce_body: dict) -> str:
    """
    https://docs.seqera.io/platform-api/create-compute-env
    """
    result = SeqeraApiClient().call(
        HTTPMethod.POST, f'/compute-envs?workspaceId={workspace_id}', ce_body
    )
    ce_id = result.get('computeEnvId')
    if not ce_id:
        raise ValueError(f'CE create did not return computeEnvId: {result!r}')
    return ce_id


def _update_compute_env_metadata(workspace_id: int, ce_id: str, name: str) -> None:
    """
    https://docs.seqera.io/platform-api/update-compute-env
    """
    SeqeraApiClient().call(
        HTTPMethod.PUT,
        f'/compute-envs/{ce_id}?workspaceId={workspace_id}',
        {'name': name},
    )


def _soft_delete_compute_env(workspace_id: int, ce_id: str, current_name: str) -> None:
    """Soft-delete a compute environment: rename with a ``-deprecated-<utc>`
    Disable compute env: https://docs.seqera.io/platform-api/disable-compute-env
    """
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    deprecated_name = f'{current_name}-deprecated-{stamp}'

    assert len(deprecated_name) <= _MAX_CE_NAME_LENGTH

    _update_compute_env_metadata(workspace_id, ce_id, deprecated_name)

    # in case of a failure of this request, we don't need to rollback previous name update.
    # Unlike in the _WorkspaceParticipantProvider.create
    SeqeraApiClient().call(
        HTTPMethod.POST,
        f'/compute-envs/{ce_id}/disable?workspaceId={workspace_id}',
        {},
    )


def _build_ce_body(inputs: ComputeEnvArgs, name_override: Optional[str] = None) -> dict:
    compute_env: dict = {
        'name': name_override or inputs.name,
        'description': inputs.description or '',
        'platform': inputs.platform,
        'credentialsId': inputs.credentials_id,
        'config': inputs.config.model_dump(by_alias=True, exclude_none=True),
    }

    body: dict = {'computeEnv': compute_env}
    if inputs.labels:
        body['labels'] = inputs.labels
    return body


# Updating these fields are restricted in the Seqera API.
# labels contains list of integers. During diff check, the same set of label ids but
# with different orders can trigger a replace event
# credentials_id is marked as an updatable field in the API but failed when testing
# Replacement attributes similar to https://registry.terraform.io/providers/seqeralabs/seqera/latest/docs/resources/gcp_batch_ce#read-only
_REPLACE_FIELDS = (
    'workspace_id',
    'description',
    'platform',
    'config',
    'labels',
    'credentials_id',
)


class _ComputeEnvProvider(ResourceProvider):
    def create(self, props: dict[str, Any]) -> CreateResult:
        inputs = ComputeEnvArgs(**props)

        ce_id = _create_compute_env(inputs.workspace_id, _build_ce_body(inputs))

        return CreateResult(id_=ce_id, outs={**props, 'compute_env_id': ce_id})

    def diff(self, _id: str, olds: dict[str, Any], news: dict[str, Any]) -> DiffResult:
        replaces: list[str] = [f for f in _REPLACE_FIELDS if olds.get(f) != news.get(f)]

        changed = bool(replaces) or (olds.get('name') != news.get('name'))
        return DiffResult(changes=changed, replaces=replaces or None)

    def update(
        self, id_: str, _olds: dict[str, Any], _news: dict[str, Any]
    ) -> UpdateResult:
        inputs = ComputeEnvArgs(**_news)

        _update_compute_env_metadata(inputs.workspace_id, id_, inputs.name)

        return UpdateResult(outs={**_news, 'compute_env_id': id_})

    def delete(self, id_: str, props: dict[str, Any]) -> None:
        _soft_delete_compute_env(int(props['workspace_id']), id_, props['name'])


class SeqeraComputeEnv(Resource):
    # Enable/Disable a compute env is not implemented
    # Set primary compute env is not implemented
    compute_env_id: pulumi.Output[str]

    def __init__(
        self,
        name: str,
        workspace_id: pulumi.Input[int],
        ce_name: pulumi.Input[str],
        credentials_id: pulumi.Input[str],
        platform: pulumi.Input[str],
        config: GoogleBatchConfig,  # Accepts only GoogleBatchConfig, extend this when supporting a new platform
        description: Optional[pulumi.Input[str]] = None,
        labels: Optional[pulumi.Input[list[int]]] = None,
        opts: Optional[pulumi.ResourceOptions] = None,
    ) -> None:
        # Seqera compute envs must have unique names.
        # In-case of a replace event (but same compute env name), there will be name collision
        # if the new compute is created while the old one exists. Therefore, delete on old executed before creating the new
        merged_opts = pulumi.ResourceOptions.merge(
            opts, pulumi.ResourceOptions(delete_before_replace=True)
        )

        super().__init__(
            _ComputeEnvProvider(),
            name,
            {
                'workspace_id': workspace_id,
                'name': ce_name,
                'credentials_id': credentials_id,
                'config': config.to_input_dict(),
                'description': description,
                'platform': platform,
                'labels': labels,
                'compute_env_id': None,
            },
            merged_opts,
        )
