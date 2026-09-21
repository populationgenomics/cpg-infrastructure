"""
Contains util functions that will be used across different Seqera credential types.
"""

from http import HTTPMethod

from cpg_infra.driver.dynamic_providers.seqera.util.api_util import SeqeraApiClient

MAX_CRED_NAME_LENGTH = 100


def create_credentials(workspace_id: int, body: dict) -> str:
    """
    https://docs.seqera.io/platform-api/create-credentials
    """
    result = SeqeraApiClient.call(
        HTTPMethod.POST,
        f'/credentials?workspaceId={workspace_id}',
        body,
    )
    cred_id = result.get('credentialsId')
    if not cred_id:
        raise ValueError(
            f'Credentials create did not return credentialsId: {result}',
        )
    return str(cred_id)


def update_credentials(workspace_id: int, cred_id: str, body: dict) -> None:
    """
    https://docs.seqera.io/platform-api/update-credentials
    """
    SeqeraApiClient.call(
        HTTPMethod.PUT,
        f'/credentials/{cred_id}?workspaceId={workspace_id}',
        body,
    )


def delete_credentials(workspace_id: int, cred_id: str) -> None:
    """
    https://docs.seqera.io/platform-api/delete-credentials
    """
    SeqeraApiClient.call(
        HTTPMethod.DELETE,
        f'/credentials/{cred_id}?workspaceId={workspace_id}',
    )
