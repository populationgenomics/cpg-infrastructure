# flake8: noqa: ANN204
"""
GroupMember class - a single member of a Group.
"""

from __future__ import annotations

from cpg_infra.config import CPGInfrastructureUser


class GroupMember:
    """
    Store both the username / cloud_id, as it's useful
    to look it up when resolving, ie for Hail Batch

    """

    def __init__(
        self,
        cloud_id: str,
        user: CPGInfrastructureUser.Cloud | None,
    ) -> None:
        self.cloud_id = cloud_id
        self.user = user

    def __lt__(
        self,
        other: GroupMember,
    ):
        return self.cloud_id < other.cloud_id

    def __repr__(self) -> str:
        members = [
            f'cloud_id={self.cloud_id!r}',
        ]
        if self.user:
            members.append(f'username={self.user.id!r}')

        return f'GroupMember({", ".join(members)})'
