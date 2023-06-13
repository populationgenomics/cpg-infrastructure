"""
Pulumi provider for interacting with metamist
"""
import functools
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

    def create(self, props) -> pulumi.dynamic.CreateResult:
        name = props['project_name']
        create_test_project = props['create_test_project']

        existing_project = get_project_by_name(name)
        if existing_project:
            return pulumi.dynamic.CreateResult(
                id_=f'metamist-project::{name}::{existing_project["id"]}',
                outs={
                    'id': existing_project['id'],
                    'name': name,
                },
            )

        project = ProjectApi().create_project(
            name=name,
            dataset=name,
            create_test_project=create_test_project,
        )

        return pulumi.dynamic.CreateResult(
            id_=f'metamist-project::{name}::{project["id"]}',
            outs={
                'id': project['id'],
                'name': name,
            },
        )

    def diff(self, _id: str, _olds, _news) -> pulumi.dynamic.DiffResult:
        replaces = []

        if _olds['project_name'] != _news['project_name']:
            replaces.append('name')

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

    def __init__(
        self,
        name: str,
        project_name: str,
        create_test_project: bool = True,
        opts: pulumi.ResourceOptions | None = None,
    ):
        args = {
            'project_name': project_name,
            'create_test_project': create_test_project,
        }
        super().__init__(MetamistProjectProvider(), name, args, opts)
