"""Dynamic provider for Seqera credentials (Supports only - Google WIF)."""

from http import HTTPMethod, HTTPStatus
from typing import Any, Optional

import pulumi
from pulumi.dynamic import (
    CreateResult,
    DiffResult,
    Resource,
    ResourceProvider,
    UpdateResult,
)

from cpg_infra.driver.dynamic_providers.seqera.inputs.credentials import (
    GoogleCredentialsArgs,
)
from cpg_infra.driver.dynamic_providers.seqera.util.api_util import (
    SeqeraApiClient,
    SeqeraAPIError,
)


def _build_keys(inputs: GoogleCredentialsArgs) -> dict:
    keys: dict = {
        'keyType': 'google',
        'workloadIdentityProvider': inputs.workload_identity_provider,
        'serviceAccountEmail': inputs.service_account_email,
    }

    if inputs.token_audience is not None:
        keys['tokenAudience'] = inputs.token_audience
    return keys


def _build_body(inputs: GoogleCredentialsArgs, cred_id: Optional[str] = None) -> dict:
    body: dict = {
        'credentials': {
            'name': inputs.name,
            'provider': 'google',
            'keys': _build_keys(inputs),
        }
    }
    if cred_id:
        body['credentials']['id'] = cred_id
    return body


def _create_credentials(inputs: GoogleCredentialsArgs) -> str | None:
    """
    https://docs.seqera.io/platform-api/create-credentials
    """

    result = SeqeraApiClient().call(
        HTTPMethod.POST,
        f'/credentials?workspaceId={inputs.workspace_id}',
        _build_body(inputs),
    )
    return result.get('credentialsId')


def _update_credentials(id_: str, inputs: GoogleCredentialsArgs) -> None:
    """
    https://docs.seqera.io/platform-api/update-credentials
    """

    SeqeraApiClient().call(
        HTTPMethod.PUT,
        f'/credentials/{id_}?workspaceId={inputs.workspace_id}',
        _build_body(inputs, cred_id=id_),
    )


def _delete_credentials(id_: str, workspace_id: int) -> None:
    """
    https://docs.seqera.io/platform-api/delete-credentials
    """
    SeqeraApiClient().call(
        HTTPMethod.DELETE, f'/credentials/{id_}?workspaceId={workspace_id}'
    )


_TRACKED_KEY_FIELDS = (
    'workload_identity_provider',
    'service_account_email',
    'token_audience',
)


def _outs(inputs: GoogleCredentialsArgs, cred_id: str) -> dict[str, Any]:
    return {
        'credentials_id': cred_id,
        'workspace_id': inputs.workspace_id,
        'name': inputs.name,
        'workload_identity_provider': inputs.workload_identity_provider,
        'service_account_email': inputs.service_account_email,
        'token_audience': inputs.token_audience,
    }


class _GoogleCredentialProvider(ResourceProvider):
    def create(self, props: dict[str, Any]) -> CreateResult:
        inputs = GoogleCredentialsArgs(**props)
        cred_id = _create_credentials(inputs)
        if not cred_id:
            raise ValueError('Credentials create did not return credentialsId')
        return CreateResult(id_=cred_id, outs=_outs(inputs, cred_id))

    def diff(self, _id: str, olds: dict[str, Any], news: dict[str, Any]) -> DiffResult:
        replaces = []
        # Replacement attributes based on https://registry.terraform.io/providers/seqeralabs/seqera/latest/docs/resources/google_credential
        if olds.get('workspace_id') != news.get('workspace_id'):
            replaces.append('workspace_id')
        if olds.get('name') != news.get('name'):
            replaces.append('name')

        changed = bool(replaces) or any(
            olds.get(f) != news.get(f) for f in _TRACKED_KEY_FIELDS
        )
        return DiffResult(changes=changed, replaces=replaces or None)

    def update(
        self, id_: str, _olds: dict[str, Any], news: dict[str, Any]
    ) -> UpdateResult:
        inputs = GoogleCredentialsArgs(**news)
        _update_credentials(id_, inputs)

        return UpdateResult(outs=_outs(inputs, id_))

    def delete(self, id_: str, props: dict[str, Any]) -> None:
        try:
            _delete_credentials(id_, int(props['workspace_id']))
        except SeqeraAPIError as e:
            if e.status_code == HTTPStatus.NOT_FOUND:
                pulumi.log.info(
                    f'Google credential {id_} already deleted/not found. Skipping delete.'
                )
            else:
                raise


class SeqeraGoogleCredentials(Resource):
    # This Dynamic Provider implementation of Seqera credentials support GCP WIF.
    # refer :
    # https://docs.seqera.io/platform-api/create-credentials
    # https://registry.terraform.io/providers/seqeralabs/seqera/latest/docs/resources/google_credential
    credentials_id: pulumi.Output[str]

    def __init__(
        self,
        name: str,
        workspace_id: pulumi.Input[int],
        cred_name: pulumi.Input[str],
        workload_identity_provider: pulumi.Input[str],
        service_account_email: pulumi.Input[str],
        token_audience: Optional[pulumi.Input[str]] = None,
        opts: Optional[pulumi.ResourceOptions] = None,
    ) -> None:
        super().__init__(
            _GoogleCredentialProvider(),
            name,
            {
                'workspace_id': workspace_id,
                'name': cred_name,
                'workload_identity_provider': workload_identity_provider,
                'service_account_email': service_account_email,
                'token_audience': token_audience,
                'credentials_id': None,
            },
            opts,
        )
