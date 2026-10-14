from typing import Literal

from pydantic import BaseModel, Field, model_validator


class WorkspaceArgs(BaseModel):
    """Validate props for the workspace dynamic resource."""

    org_id: int
    workspace_id: int
    name: str = Field(min_length=2, max_length=40)
    full_name: str = Field(min_length=1, max_length=100)
    visibility: Literal['PRIVATE', 'SHARED'] = 'PRIVATE'
    description: str | None = Field(None, max_length=1000)


WorkspaceParticipantRole = Literal[
    'owner', 'admin', 'maintain', 'launch', 'connect', 'view'
]


class WorkspaceParticipantArgs(BaseModel):
    """Validate props for the workspace-participant dynamic resource."""

    org_id: int
    workspace_id: int
    role: WorkspaceParticipantRole
    email: str | None = None
    member_id: int | None = None
    team_id: int | None = None
    participant_id: int | None = None

    @model_validator(mode='after')
    def _exactly_one_subject(self) -> 'WorkspaceParticipantArgs':
        # validation similar to https://registry.terraform.io/providers/seqeralabs/seqera/latest/docs/resources/workspace_participant
        provided = sum(
            1 for x in (self.email, self.member_id, self.team_id) if x is not None
        )
        if provided != 1:
            raise ValueError(
                'SeqeraWorkspaceParticipant requires exactly one of '
                "'email', 'member_id', or 'team_id'."
            )
        return self
