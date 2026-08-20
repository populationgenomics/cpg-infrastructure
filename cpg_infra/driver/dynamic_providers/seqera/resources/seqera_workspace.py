"""Dynamic provider for Seqera workspaces."""

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

from cpg_infra.driver.dynamic_providers.seqera.inputs.workspace import (
    WorkspaceArgs,
)
from cpg_infra.driver.dynamic_providers.seqera.util.api_util import call_seqera_api


def _describe_workspace(org_id: int, workspace_id: int) -> dict:
    """Fetch workspace information.
    https://docs.seqera.io/platform-api/describe-workspace
    """
    result = call_seqera_api(
        HTTPMethod.GET, f'/orgs/{org_id}/workspaces/{workspace_id}'
    )
    return result.get('workspace') or result


def _update_workspace(inputs: WorkspaceArgs) -> None:
    """Update workspace information.
    https://docs.seqera.io/platform-api/update-workspace
    """
    call_seqera_api(
        HTTPMethod.PUT,
        f'/orgs/{inputs.org_id}/workspaces/{inputs.workspace_id}',
        {
            'name': inputs.name,
            'fullName': inputs.full_name,
            'description': inputs.description or '',
            'visibility': inputs.visibility,
        },
    )


def _compare_imported_state(ws: dict, inputs: WorkspaceArgs) -> dict:
    """Compare the imported workspace state against the code-declared inputs."""
    imported_state = {
        'name': ws.get('name'),
        'full_name': ws.get('fullName'),
        'description': ws.get('description') or '',
        'visibility': ws.get('visibility'),
    }
    difference = [
        f'{field}: imported={imported_state[field]!r} code={getattr(inputs, field)!r}'
        for field in ('name', 'full_name', 'description', 'visibility')
        if getattr(inputs, field) != imported_state[field]
    ]
    if difference:
        pulumi.log.warn(
            f"Imported workspace {inputs.workspace_id} drift from code — "
            f"next `pulumi up` will reconcile: {'; '.join(difference)}"
        )
    return imported_state


class _WorkspaceProvider(ResourceProvider):
    def create(self, props: dict[str, Any]) -> CreateResult:
        """Performs a read-only import of an existing workspace into the Pulumi state.
        Any attribute differences between the code and live resource are
        ignored during this step. A subsequent `pulumi up` will apply the local
        definitions to the live resource as an update.

        https://docs.seqera.io/platform-api/create-workspace
        """
        inputs = WorkspaceArgs(**props)

        ws = _describe_workspace(inputs.org_id, inputs.workspace_id)
        imported_state = _compare_imported_state(ws, inputs)

        return CreateResult(
            id_=str(inputs.workspace_id),
            outs={
                'org_id': inputs.org_id,
                'workspace_id': inputs.workspace_id,
                **imported_state,
            },
        )

    def diff(self, _id: str, olds: dict[str, Any], news: dict[str, Any]) -> DiffResult:
        replaces = []
        # Diff in the workspace id is treated as a replacement.
        # But as per the current implementation, create -> imports workspace, delete -> no-op
        if olds.get('workspace_id') != news.get('workspace_id'):
            replaces.append('workspace_id')

        changed = bool(replaces) or (
            olds.get('name') != news.get('name')
            or olds.get('description') != news.get('description')
            or olds.get('full_name') != news.get('full_name')
            or olds.get('visibility') != news.get('visibility')
        )
        return DiffResult(changes=changed, replaces=replaces or None)

    def update(
        self, id_: str, _olds: dict[str, Any], news: dict[str, Any]
    ) -> UpdateResult:
        """
        https://docs.seqera.io/platform-api/update-workspace
        """
        inputs = WorkspaceArgs(**{**news, 'workspace_id': int(id_)})
        _update_workspace(inputs)
        return UpdateResult(outs={**news, 'workspace_id': inputs.workspace_id})

    def delete(self, id_: str, props: dict[str, Any]) -> None:
        # Workspace deletion is intentionally ignored as it will not be handled via IaC.
        pulumi.log.warn(
            f"Skipping DELETE of workspace {id_} ('{props.get('name')}'): "
            "workspace deletion via IaC is out of scope. Delete "
            "manually from the UI or API if required."
        )


class SeqeraWorkspace(Resource):
    workspace_id: pulumi.Output[int]

    # Import the live workspace
    # If the specified workspace does not exist, throws an error
    def __init__(
        self,
        name: str,
        org_id: pulumi.Input[int],
        # Require workspace_id as an argument, as we will be importing
        workspace_id: pulumi.Input[int],
        ws_name: pulumi.Input[str],
        full_name: pulumi.Input[str],
        visibility: pulumi.Input[str],
        description: Optional[pulumi.Input[str]] = None,
        opts: Optional[pulumi.ResourceOptions] = None,
    ) -> None:
        super().__init__(
            _WorkspaceProvider(),
            name,
            {
                'org_id': org_id,
                'workspace_id': workspace_id,
                'name': ws_name,
                'full_name': full_name,
                'visibility': visibility,
                'description': description,
            },
            opts,
        )
