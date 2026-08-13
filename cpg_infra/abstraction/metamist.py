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
        meta = props.get('meta') or {}

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

        # create_project doesn't carry meta, so sync it with a follow-up call.
        # Applies to both newly-created and pre-existing projects.
        if meta:
            ProjectApi().update_project(name, {'meta': meta})

        return pulumi.dynamic.CreateResult(
            id_=f'metamist-project::{name}::{project_id}',
            outs={
                'project_id': project_id,
                'project_name': name,
                'meta': meta,
            },
        )

    def update(self, _id: str, _olds, _news) -> pulumi.dynamic.UpdateResult:
        name = _news['project_name']
        old_meta = _olds.get('meta') or {}
        new_meta = _news.get('meta') or {}

        # The cpg-infra-private config is the source of truth for these meta keys:
        # we sync config -> metamist, but never metamist -> config. So to change a
        # display_name or description, edit the dataset config rather than editing
        # the project meta directly in metamist.
        #
        # The server merge-patches meta, so a key dropped from the config would
        # otherwise linger. Send an explicit null for those keys to clear them.

        #  new state stores it under 'project_id' (commit 295359f onwards)
        #  state from before that rename stored it under 'id'
        #  if neither is present (e.g. stale refresh), re-fetch by name
        project_id = _olds.get('project_id') or _olds.get('id')
        if project_id is None:
            project = get_project_by_name(name)
            if not project:
                raise RuntimeError(f'Project {name} not found in metamist')
            project_id = project['id']

        meta_update = dict(new_meta)
        for k in old_meta.keys() - new_meta.keys():
            meta_update[k] = None  # Tell update_project() to remove this entry

        if meta_update:
            ProjectApi().update_project(name, {'meta': meta_update})

        return pulumi.dynamic.UpdateResult(
            outs={
                'project_id': project_id,
                'project_name': name,
                'meta': new_meta,
            },
        )

    def diff(self, _id: str, _olds, _news) -> pulumi.dynamic.DiffResult:
        replaces = []

        if _olds['project_name'] != _news['project_name']:
            replaces.append('project_name')

        # A meta-only change is an in-place update (never a project replace).
        meta_changed = (_olds.get('meta') or {}) != (_news.get('meta') or {})

        return pulumi.dynamic.DiffResult(
            changes=len(replaces) > 0 or meta_changed,
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
    """Create a metamist project and sync its meta"""

    project_id: pulumi.Output[int]
    project_name: pulumi.Output[str]
    # Pulumi's translate_output_properties can't destructure a PEP 604 union
    # (str | list[str]) inside a generic — it raises AssertionError when a
    # dict value happens to be a list. `dict[str, Any]` is the tightest
    # runtime-safe annotation; the constructor parameter below keeps the
    # narrower contract for callers and type checkers.
    meta: pulumi.Output[dict[str, Any]]

    def __init__(
        self,
        name: str,
        project_name: str,
        meta: dict[str, str | list[str]] | None = None,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        args = {
            'project_name': project_name,
            'meta': meta or {},
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
