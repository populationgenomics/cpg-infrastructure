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

from cpg_infra.driver.dynamic_providers.seqera.inputs.workspace import (
    WorkspaceParticipantArgs,
)
from cpg_infra.driver.dynamic_providers.seqera.util.api_util import (
    SeqeraApiClient,
    SeqeraAPIError,
)


def _build_create_participant_body(inputs: WorkspaceParticipantArgs) -> dict:
    if inputs.member_id is not None:
        return {'memberId': inputs.member_id}
    if inputs.email is not None:
        return {'userNameOrEmail': inputs.email}
    return {'teamId': inputs.team_id}


def _create_participant(inputs: WorkspaceParticipantArgs) -> dict:
    """
    https://docs.seqera.io/platform-api/create-workspace-participant
    """
    created_participant = SeqeraApiClient().call(
        HTTPMethod.PUT,
        f'/orgs/{inputs.org_id}/workspaces/{inputs.workspace_id}/participants/add',
        _build_create_participant_body(inputs),
    )
    return created_participant.get('participant') or {}


def _update_participant_role(
    inputs: WorkspaceParticipantArgs, participant_id: int
) -> None:
    """
    https://docs.seqera.io/platform-api/update-workspace-participant-role
    """
    SeqeraApiClient().call(
        HTTPMethod.PUT,
        f'/orgs/{inputs.org_id}/workspaces/{inputs.workspace_id}'
        f'/participants/{participant_id}/role',
        {'role': inputs.role},
    )


def _delete_participant(org_id: int, workspace_id: int, participant_id: int) -> None:
    """
    https://docs.seqera.io/platform-api/delete-workspace-participant
    """

    SeqeraApiClient().call(
        HTTPMethod.DELETE,
        f'/orgs/{org_id}'
        f'/workspaces/{workspace_id}'
        f'/participants/{participant_id}',
    )


class _WorkspaceParticipantProvider(ResourceProvider):
    def create(self, props: dict[str, Any]) -> CreateResult:
        """
        Adds a participant to a workspace and assign the requested role to them.
        """
        inputs = WorkspaceParticipantArgs(**props)

        participant = _create_participant(inputs)
        participant_id = participant.get('participantId')

        assert isinstance(participant_id, int)

        created_role = participant.get('wspRole', '')

        # Update the role if desired role is different from the created role
        # The current https://docs.seqera.io/platform-api/create-workspace-participant api does not accept a role
        # and assign Launch as default
        if created_role.lower() != inputs.role:
            try:
                _update_participant_role(inputs, participant_id)
            except SeqeraAPIError:
                # Roll back, as next create (pulumi up) will fail as the participant already exist
                # Another option - instead of deleting, fetch the existing participant to update the state.
                # but there is no option to retrieve a specific participant
                # https://docs.seqera.io/platform-api/list-workspace-participants fetches all in a workspace.
                try:
                    _delete_participant(
                        inputs.org_id, inputs.workspace_id, participant_id
                    )
                except SeqeraAPIError as e:
                    pulumi.log.warn(
                        f'Failed to roll back participant {participant_id} '
                        f'after role update failed: {e}. Remove participant to reconcile'
                    )
                raise

        return CreateResult(
            id_=str(participant_id),
            outs={**props, 'participant_id': participant_id},
        )

    def diff(self, _id: str, olds: dict[str, Any], news: dict[str, Any]) -> DiffResult:
        # Diff in the workspace id is treated as a replacement.
        replaces = [
            f
            for f in ('workspace_id', 'email', 'member_id', 'team_id')
            if olds.get(f) != news.get(f)
        ]
        changed = bool(replaces) or olds.get('role') != news.get('role')
        return DiffResult(changes=changed, replaces=replaces or None)

    def update(
        self, id_: str, _olds: dict[str, Any], news: dict[str, Any]
    ) -> UpdateResult:
        # Once added to the workspace, we can only change the role

        inputs = WorkspaceParticipantArgs(**news)
        participant_id = int(id_)

        _update_participant_role(inputs, participant_id)
        return UpdateResult(outs={**news, 'participant_id': participant_id})

    def delete(self, id_: str, props: dict[str, Any]) -> None:
        try:
            _delete_participant(
                int(props['org_id']), int(props['workspace_id']), int(id_)
            )
        except SeqeraAPIError as e:
            if e.status_code == HTTPStatus.NOT_FOUND:
                pulumi.log.info(
                    f'Workspace participant {id_} already deleted/not found. Skipping delete.'
                )
            else:
                raise


class SeqeraWorkspaceParticipant(Resource):
    participant_id: pulumi.Output[int]

    def __init__(
        self,
        name: str,
        org_id: pulumi.Input[int],
        workspace_id: pulumi.Input[int],
        role: pulumi.Input[str],
        email: Optional[pulumi.Input[str]] = None,
        member_id: Optional[pulumi.Input[int]] = None,
        team_id: Optional[pulumi.Input[int]] = None,
        opts: Optional[pulumi.ResourceOptions] = None,
    ) -> None:
        super().__init__(
            _WorkspaceParticipantProvider(),
            name,
            {
                'org_id': org_id,
                'workspace_id': workspace_id,
                'email': email,
                'member_id': member_id,
                'team_id': team_id,
                'role': role,
                'participant_id': None,
            },
            opts,
        )
