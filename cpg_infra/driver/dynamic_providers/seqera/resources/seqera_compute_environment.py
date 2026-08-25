"""Dynamic provider for Seqera compute environments."""

import uuid
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
    MAX_CE_NAME_LENGTH,
    MAX_CRED_NAME_LENGTH,
    ComputeEnvArgs,
    GoogleBatchConfig,
    GoogleWifCredentialArgs,
    GoogleWifCredentialConfig,
)
from cpg_infra.driver.dynamic_providers.seqera.util.api_util import (
    SeqeraApiClient,
)

_CRED_UUID_LEN = 8


def _generate_credentials_name(ce_name: str) -> str:
    """Generate a unique credential name from the CE name + short uuid suffix."""
    suffix = f'-cred-{uuid.uuid4().hex[:_CRED_UUID_LEN]}'
    combined = f'{ce_name}{suffix}'
    assert len(combined) <= MAX_CRED_NAME_LENGTH
    return combined


def _build_credentials_body(
    creds: GoogleWifCredentialArgs, name: str, cred_id: Optional[str] = None
) -> dict:
    keys: dict = {
        'keyType': 'google',
        **creds.model_dump(by_alias=True, exclude_none=True, exclude={'id', 'name'}),
    }
    body: dict = {
        'credentials': {
            'name': name,
            'provider': 'google',
            'keys': keys,
        }
    }
    if cred_id:
        body['credentials']['id'] = cred_id
    return body


def _create_credentials(
    workspace_id: int, creds: GoogleWifCredentialArgs, name: str
) -> str:
    """https://docs.seqera.io/platform-api/create-credentials"""
    result = SeqeraApiClient().call(
        HTTPMethod.POST,
        f'/credentials?workspaceId={workspace_id}',
        _build_credentials_body(creds, name),
    )
    cred_id = result.get('credentialsId')
    if not cred_id:
        raise ValueError(
            f'Compute env credentials create did not return credentialsId: {result}'
        )
    return str(cred_id)


def _update_credentials(workspace_id: int, creds: GoogleWifCredentialArgs) -> None:
    """https://docs.seqera.io/platform-api/update-credentials"""

    assert creds.id is not None and creds.name is not None
    SeqeraApiClient().call(
        HTTPMethod.PUT,
        f'/credentials/{creds.id}?workspaceId={workspace_id}',
        _build_credentials_body(creds, creds.name, creds.id),
    )


def _create_compute_env(workspace_id: int, ce_body: dict) -> str:
    """https://docs.seqera.io/platform-api/create-compute-env"""
    result = SeqeraApiClient().call(
        HTTPMethod.POST, f'/compute-envs?workspaceId={workspace_id}', ce_body
    )
    ce_id = result.get('computeEnvId')
    if not ce_id:
        raise ValueError(f'Compute env create did not return computeEnvId: {result}')
    return str(ce_id)


def _update_compute_env_metadata(workspace_id: int, ce_id: str, name: str) -> None:
    """https://docs.seqera.io/platform-api/update-compute-env"""
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

    assert len(deprecated_name) <= MAX_CE_NAME_LENGTH

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
        'credentialsId': inputs.credentials.id,
        'config': inputs.config.model_dump(by_alias=True, exclude_none=True),
    }

    body: dict = {'computeEnv': compute_env}
    if inputs.label_ids:
        body['labelIds'] = inputs.label_ids
    return body


_CRED_UPDATE_FIELDS = (
    'workload_identity_provider',
    'service_account_email',
    'token_audience',
)


# Updating these fields are restricted in the Seqera API.
# labels contains list of integers. During diff check, the same set of label ids but
# with different orders can trigger a replace event
# Replacement attributes similar to https://registry.terraform.io/providers/seqeralabs/seqera/latest/docs/resources/gcp_batch_ce#read-only
_CE_REPLACE_FIELDS = (
    'workspace_id',
    'description',
    'platform',
    'config',
    'label_ids',
)


class _ComputeEnvProvider(ResourceProvider):
    def create(self, props: dict[str, Any]) -> CreateResult:
        """
        Creates Compute environment credentials and the compute env
        """
        inputs = ComputeEnvArgs(**props)

        cred_name = _generate_credentials_name(inputs.name)
        cred_id = _create_credentials(
            inputs.workspace_id, inputs.credentials, cred_name
        )

        inputs.credentials.id = cred_id
        inputs.credentials.name = cred_name
        ce_id = _create_compute_env(inputs.workspace_id, _build_ce_body(inputs))

        return CreateResult(
            id_=ce_id,
            outs={
                **props,
                'compute_env_id': ce_id,
                'credentials': inputs.credentials.model_dump(exclude_none=True),
            },
        )

    def diff(self, _id: str, olds: dict[str, Any], news: dict[str, Any]) -> DiffResult:
        replaces: list[str] = [
            f for f in _CE_REPLACE_FIELDS if olds.get(f) != news.get(f)
        ]

        old_creds = olds['credentials']
        new_creds = news['credentials']
        cred_changed = any(
            old_creds.get(f) != new_creds.get(f) for f in _CRED_UPDATE_FIELDS
        )
        ce_name_changed = olds.get('name') != news.get('name')

        changed = bool(replaces) or cred_changed or ce_name_changed
        return DiffResult(changes=changed, replaces=replaces or None)

    def update(
        self, id_: str, olds: dict[str, Any], news: dict[str, Any]
    ) -> UpdateResult:
        inputs = ComputeEnvArgs(**news)

        _old_creds = olds['credentials']
        inputs.credentials.id = _old_creds['id']
        inputs.credentials.name = _old_creds['name']

        assert inputs.credentials.id and inputs.credentials.name

        cred_changed = any(
            _old_creds.get(f) != news['credentials'].get(f) for f in _CRED_UPDATE_FIELDS
        )
        if cred_changed:
            _update_credentials(inputs.workspace_id, inputs.credentials)

        if olds.get('name') != news.get('name'):
            _update_compute_env_metadata(inputs.workspace_id, id_, inputs.name)

        return UpdateResult(
            outs={
                **news,
                'compute_env_id': id_,
                'credentials': inputs.credentials.model_dump(exclude_none=True),
            }
        )

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
        platform: pulumi.Input[str],
        config: GoogleBatchConfig,  # Extend when supporting new platforms
        credentials: GoogleWifCredentialConfig,
        description: Optional[pulumi.Input[str]] = None,
        label_ids: Optional[pulumi.Input[list[int]]] = None,
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
                'credentials': credentials.to_input_dict(),
                'config': config.to_input_dict(),
                'description': description,
                'platform': platform,
                'label_ids': label_ids,
                'compute_env_id': None,
            },
            merged_opts,
        )
