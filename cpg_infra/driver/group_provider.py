# flake8: noqa: ANN204
"""
GroupProvider - manages Group instances and their memberships across clouds.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

import graphlib

# Imported under an underscore-prefix alias so the outer name is not shadowed
# by the ``Group`` class attribute below. Internal references use ``_Group``
# so mypy resolves them unambiguously; external callers still see
# ``GroupProvider.Group`` via the class attribute.
from cpg_infra.driver.group import Group as _Group

if TYPE_CHECKING:
    from cpg_infra.abstraction.base import CloudInfraBase
    from cpg_infra.config import CloudName
    from cpg_infra.driver.group_member import GroupMember


class GroupProvider:
    """Provider for managing groups + memberships"""

    # Legacy nested-class access paths: `GroupProvider.Group` /
    # `GroupProvider.Group.GroupMember`
    Group = _Group

    def __init__(self, group_prefix: str | None = None) -> None:
        self.groups: dict[
            CloudName,
            dict[str, _Group],
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
    ) -> _Group:
        if infra.name() not in self.groups:
            self.groups[infra.name()] = {}
        if name in self.groups[infra.name()]:
            raise ValueError(f'Group "{name}" in "{infra.name()}" already exists')

        group = _Group(
            name=name,
            cache_members=cache_members,
            members=members or {},
            group=infra.create_group(self.group_prefix + name, description),
        )
        self.groups[infra.name()][name] = group

        return group

    def static_group_order(self, cloud: CloudName) -> list[_Group]:
        """
        not that it super matters because we do recursively look it up and
        cache the result, but it's nice to grab the groups in an order that
        minimises depth looking.
        """
        groups = self.groups[cloud]

        deps = {
            group.name: [
                g.name for g in group.members.values() if isinstance(g, _Group)
            ]
            for group in groups.values()
        }

        return [groups[n] for n in graphlib.TopologicalSorter(deps).static_order()]

    def resolve_group_members(
        self,
        group: _Group,
    ) -> list[GroupMember]:
        if group.name in self._cached_resolved_members:
            return self._cached_resolved_members[group.name]

        resolved_members: list[GroupMember] = []
        for member in group.members.values():
            if isinstance(member, _Group):
                resolved_members.extend(self.resolve_group_members(member))
            else:
                resolved_members.append(member)

        self._cached_resolved_members[group.name] = list(set(resolved_members))
        return resolved_members
