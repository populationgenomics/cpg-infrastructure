# flake8: noqa: ANN204
"""
Group data structures: GroupMember, Group, and GroupProvider.
"""

from __future__ import annotations

import graphlib
from collections import defaultdict
from typing import TYPE_CHECKING

from cpg_infra.config import CPGInfrastructureUser

if TYPE_CHECKING:
    from cpg_infra.abstraction.base import CloudInfraBase
    from cpg_infra.config import CloudName


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



class Group:
    """Placeholder for a Group of members"""

    # duck-type marker read by `cpg_infra/abstraction/{gcp,azure}.py`
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
            self.members[resource_key] = GroupMember(member, user)
        else:
            if user:
                raise ValueError(
                    f'Invalid user type {type(user)} ({user}) for member '
                    f'{member} for {resource_key}',
                )
            self.members[resource_key] = GroupMember(member, None)

    def __repr__(self) -> str:
        return f'Group({self.name!r})'



class GroupProvider:
    """Provider for managing groups + memberships"""

    def __init__(self, group_prefix: str | None = None) -> None:
        self.groups: dict[
            CloudName,
            dict[str, Group],
        ] = defaultdict()

        self.group_prefix = group_prefix or ''
        self._cached_resolved_members: dict[str, list] = {}

    def get_group(self, infra_name: CloudName, group_name: str):
        return self.groups[infra_name][group_name]

    def create_group(
        self,
        infra: CloudInfraBase,
        name: str,
        cache_members: bool,
        members: dict | None = None,
        description: str | None = None,
    ) -> Group:
        if infra.name() not in self.groups:
            self.groups[infra.name()] = {}
        if name in self.groups[infra.name()]:
            raise ValueError(f'Group "{name}" in "{infra.name()}" already exists')

        group = Group(
            name=name,
            cache_members=cache_members,
            members=members or {},
            group=infra.create_group(self.group_prefix + name, description),
        )
        self.groups[infra.name()][name] = group

        return group

    def static_group_order(self, cloud: CloudName) -> list[Group]:
        """
        not that it super matters because we do recursively look it up and
        cache the result, but it's nice to grab the groups in an order that
        minimises depth looking.
        """
        groups = self.groups[cloud]

        deps = {
            group.name: [
                g.name for g in group.members.values() if isinstance(g, Group)
            ]
            for group in groups.values()
        }

        return [groups[n] for n in graphlib.TopologicalSorter(deps).static_order()]

    def resolve_group_members(
        self,
        group: Group,
    ) -> list[GroupMember]:
        if group.name in self._cached_resolved_members:
            return self._cached_resolved_members[group.name]

        resolved_members: list[GroupMember] = []
        for member in group.members.values():
            if isinstance(member, Group):
                resolved_members.extend(self.resolve_group_members(member))
            else:
                resolved_members.append(member)

        self._cached_resolved_members[group.name] = list(set(resolved_members))
        return resolved_members
