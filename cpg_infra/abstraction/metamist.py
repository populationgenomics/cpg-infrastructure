# flake8: noqa: ANN001
"""
Pulumi provider for interacting with metamist
"""

import functools
from collections import defaultdict
from typing import Any

import pulumi

from metamist.apis import ProjectApi


@functools.cache
def get_projects() -> dict[str, dict]:
    """
    Get all projects from metamist, useful to avoid repeated calls to the API
    :return:
    """
    api = ProjectApi()
    all_projects = api.get_all_projects()
    return {p['name']: p for p in all_projects}


def get_project_by_name(name: str) -> dict | None:
    """
    Get a project by name from metamist, uses the cached get_projects() function
    """
    projects = get_projects()
    return projects.get(name)


class MetamistProjectProvider(pulumi.dynamic.ResourceProvider):
    """Pulumi provider for creating a metamist project"""

    def create(self, props: dict[str, Any]) -> pulumi.dynamic.CreateResult:
        name = props['project_name']

        if project := get_project_by_name(name):
            project_id = project['id']
        else:
            project_id = ProjectApi().create_project(
                name=name,
                dataset=name,
                create_test_project=False,
            )

        if not project_id:
            raise RuntimeError(f'Failed to create project {name}')

        return pulumi.dynamic.CreateResult(
            id_=f'metamist-project::{name}::{project_id}',
            outs={
                'project_id': project_id,
                'project_name': name,
            },
        )

    def diff(self, _id: str, _olds, _news) -> pulumi.dynamic.DiffResult:
        replaces = []

        if _olds['project_name'] != _news['project_name']:
            replaces.append('project_name')

        return pulumi.dynamic.DiffResult(
            changes=len(replaces) > 0,
            replaces=replaces,
            delete_before_replace=len(replaces) > 0,
        )

    def delete(self, _id: str, _props) -> None:
        # don't delete projects
        pass

    def read(self, id_: str, props) -> pulumi.dynamic.ReadResult:
        project = get_project_by_name(props['project_name'])
        if not project:
            return pulumi.dynamic.ReadResult(None, {})

        return pulumi.dynamic.ReadResult(id_=id_, outs=props)


class MetamistProject(pulumi.dynamic.Resource):
    """Create a membership to a Hail Batch Billing Project"""

    project_id: pulumi.Output[int]
    project_name: pulumi.Output[str]

    def __init__(
        self,
        name: str,
        project_name: str,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        args = {
            'project_name': project_name,
        }
        super().__init__(MetamistProjectProvider(), name, args, opts)


class MetamistProjectMembersProvider(pulumi.dynamic.ResourceProvider):
    """Pulumi provider for creating a metamist project"""

    def create(self, props) -> pulumi.dynamic.CreateResult:
        project_name = props['project_name']
        read_members = props['read_members']
        write_members = props['write_members']
        contribute_members = props['contribute_members']

        member_roles = [
            (read_members, 'reader'),
            (write_members, 'writer'),
            (contribute_members, 'contributor'),
        ]

        project_member_dict: defaultdict[str, set[str]] = defaultdict(set)

        for member_list, role in member_roles:
            for member in member_list:
                project_member_dict[member].add(role)

        project_member_update = [
            {'member': member, 'roles': list(roles)}
            for member, roles in project_member_dict.items()
        ]

        papi = ProjectApi()
        papi.update_project_members(
            project=project_name,
            project_member_update=project_member_update,
        )
        return pulumi.dynamic.CreateResult(
            id_=f'metamist-project-members::{project_name}',
            outs={**props},
        )

    def diff(self, _id: str, _olds, _news) -> pulumi.dynamic.DiffResult:
        replaces = []

        for k in 'read_members', 'write_members', 'contribute_members':
            if _olds.get(k) != _news.get(k):
                replaces.append(k)

        return pulumi.dynamic.DiffResult(
            changes=len(replaces) > 0,
            replaces=replaces,
            delete_before_replace=False,
        )

    def delete(self, _id: str, _props) -> None:
        # don't delete projects
        pass

    def read(self, id_: str, props) -> pulumi.dynamic.ReadResult:
        return pulumi.dynamic.ReadResult(id_=id_, outs=props)


class MetamistProjectMembers(pulumi.dynamic.Resource):
    """Add members to a metamist project"""

    def __init__(
        self,
        name: str,
        metamist_project_name: str | pulumi.Output[str],
        read_members: list[str] | pulumi.Output[list[str]],
        write_members: list[str] | pulumi.Output[list[str]],
        contribute_members: list[str] | pulumi.Output[list[str]],
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        args = {
            'project_name': metamist_project_name,
            'read_members': read_members,
            'write_members': write_members,
            'contribute_members': contribute_members,
        }
        super().__init__(MetamistProjectMembersProvider(), name, args, opts)
