"""Dynamic provider for a Seqera GitHub credential."""

from http import HTTPStatus
from typing import Any, Optional

import pulumi
from google.cloud import secretmanager
from pulumi.dynamic import (
    CreateResult,
    DiffResult,
    Resource,
    ResourceProvider,
    UpdateResult,
)

from cpg_infra.driver.dynamic_providers.seqera.inputs.credentials import (
    GithubCredentialArgs,
)
from cpg_infra.driver.dynamic_providers.seqera.util.api_util import (
    SeqeraAPIError,
)
from cpg_infra.driver.dynamic_providers.seqera.util.credentials_util import (
    create_credentials,
    delete_credentials,
    update_credentials,
)


def _latest_ref(secret_name: str) -> str:
    return f'{secret_name}/versions/latest'


def _resolve_latest_version(secret_name: str) -> str:
    """Return the resource name of the latest version"""
    client = secretmanager.SecretManagerServiceClient()
    version = client.get_secret_version(request={'name': _latest_ref(secret_name)})
    return version.name


def _access_latest_token(secret_name: str) -> tuple[str, str]:
    """Return the secret payload"""
    client = secretmanager.SecretManagerServiceClient()
    resp = client.access_secret_version(request={'name': _latest_ref(secret_name)})
    return resp.payload.data.decode('utf-8'), resp.name


def _build_body(
    inputs: GithubCredentialArgs,
    token: str,
    cred_id: Optional[str] = None,
) -> dict:
    credentials: dict = {
        'name': inputs.name,
        'provider': 'github',
        'keys': {'username': inputs.username, 'password': token},
        'baseUrl': inputs.base_url,
    }
    if cred_id:
        credentials['id'] = cred_id
    return {'credentials': credentials}


def _outs(
    inputs: GithubCredentialArgs,
    cred_id: str,
    resolved_secret_version: str,
) -> dict[str, Any]:
    return {
        'workspace_id': inputs.workspace_id,
        'name': inputs.name,
        'username': inputs.username,
        'base_url': inputs.base_url,
        'access_token_secret_name': inputs.access_token_secret_name,
        'resolved_secret_version': resolved_secret_version,
        'credentials_id': cred_id,
    }


# Fields triggering replacement
_REPLACE_FIELDS = ('workspace_id', 'name')
# Fields triggering in-place update
_UPDATE_FIELDS = ('username', 'base_url', 'access_token_secret_name')


class _GithubCredentialProvider(ResourceProvider):
    def create(self, props: dict[str, Any]) -> CreateResult:
        inputs = GithubCredentialArgs(**props)
        token, resolved_version = _access_latest_token(inputs.access_token_secret_name)
        cred_id = create_credentials(inputs.workspace_id, _build_body(inputs, token))
        return CreateResult(
            id_=cred_id,
            outs=_outs(inputs, cred_id, resolved_version),
        )

    def diff(self, _id: str, olds: dict[str, Any], news: dict[str, Any]) -> DiffResult:
        replaces = [f for f in _REPLACE_FIELDS if olds.get(f) != news.get(f)]
        field_changed = any(olds.get(f) != news.get(f) for f in _UPDATE_FIELDS)

        # Update Seqera token if there is a new version in GCP secret manager
        current_version = _resolve_latest_version(news['access_token_secret_name'])
        secret_rotated = current_version != olds.get('resolved_secret_version')

        changed = bool(replaces) or field_changed or secret_rotated
        return DiffResult(changes=changed, replaces=replaces or None)

    def update(
        self,
        id_: str,
        _olds: dict[str, Any],
        news: dict[str, Any],
    ) -> UpdateResult:
        inputs = GithubCredentialArgs(**news)
        token, resolved_version = _access_latest_token(inputs.access_token_secret_name)
        update_credentials(
            inputs.workspace_id,
            id_,
            _build_body(inputs, token, cred_id=id_),
        )
        return UpdateResult(outs=_outs(inputs, id_, resolved_version))

    def delete(self, id_: str, props: dict[str, Any]) -> None:
        try:
            delete_credentials(int(props['workspace_id']), id_)
        except SeqeraAPIError as e:
            if e.status_code == HTTPStatus.NOT_FOUND:
                pulumi.log.info(
                    f'GitHub credential {id_} already deleted/not found. '
                    'Skipping delete.',
                )
                return
            raise


class SeqeraGithubCredential(Resource):
    credentials_id: pulumi.Output[str]

    def __init__(
        self,
        name: str,
        workspace_id: pulumi.Input[int],
        cred_name: pulumi.Input[str],
        username: pulumi.Input[str],
        access_token_secret_name: pulumi.Input[str],
        base_url: pulumi.Input[str],
        opts: Optional[pulumi.ResourceOptions] = None,
    ) -> None:
        super().__init__(
            _GithubCredentialProvider(),
            name,
            {
                'workspace_id': workspace_id,
                'name': cred_name,
                'username': username,
                'access_token_secret_name': access_token_secret_name,
                'base_url': base_url,
                'resolved_secret_version': None,
                'credentials_id': None,
            },
            opts,
        )
