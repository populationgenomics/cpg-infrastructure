# flake8: noqa: ANN204
"""
Group class - collection of members and/or sub-groups.
"""

from __future__ import annotations

from cpg_infra.config import CPGInfrastructureUser
from cpg_infra.driver.group_member import GroupMember


class Group:
    """Placeholder for a Group of members"""

    # Legacy nested-class access path: `Group.GroupMember`
    GroupMember = GroupMember

    # useful for checking isinstance without isinstance
    is_group = True

    def __init__(
        self,
        name: str,
        group: Group,
        members: dict,
        cache_members: bool,
    ) -> None:
        self.name: str = name
        self.group: Group = group
        self.cache_members: bool = cache_members
        self.members: dict[
            str,
            GroupMember | Group,
        ] = members

    def add_member(
        self,
        resource_key: str,
        member: str | Group,
        user: CPGInfrastructureUser.Cloud | None = None,
    ):
        if isinstance(member, type(self)) and member.name == self.name:
            raise ValueError(f'Cannot add self to group {self.name}')

        if isinstance(member, Group):
            self.members[resource_key] = member
        elif isinstance(user, CPGInfrastructureUser.Cloud):
            self.members[resource_key] = self.GroupMember(member, user)
        else:
            if user:
                raise ValueError(
                    f'Invalid user type {type(user)} ({user}) for member '
                    f'{member} for {resource_key}',
                )
            self.members[resource_key] = self.GroupMember(member, None)

    def __repr__(self) -> str:
        return f'Group({self.name!r})'
