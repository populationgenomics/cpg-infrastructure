"""
Resolve the Themis ACL file from config.

Themis reads one JSON object, the ACL file, and nothing else from this
infrastructure. It holds Themis's access control lists: one ACL for the
deployment (`admitted`, the people Themis admits) and, for each Project, its
datasets and an ACL of its members. Themis owns the format: it rejects a file
whose `format` it does not know, whose emails are not lowercase, or that lists
a Project member missing from `admitted`. `CPGInfrastructure.setup_themis`
writes the result.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from cpg_infra.abstraction.gcp import GcpInfrastructure

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cpg_infra.config import (
        CPGDatasetConfig,
        CPGInfrastructureConfig,
        CPGInfrastructureUser,
        MemberKey,
    )

ACLS_FORMAT = 1
ACLS_OBJECT_NAME = 'acls.json'


def environment_projects(
    projects: Mapping[str, CPGInfrastructureConfig.Themis.Project],
    environment: str,
) -> dict[str, CPGInfrastructureConfig.Themis.Project]:
    """The Projects whose `environments` include `environment`."""
    return {
        project_id: project
        for project_id, project in projects.items()
        if environment in project.environments
    }


def themis_datasets(
    projects: Mapping[str, CPGInfrastructureConfig.Themis.Project],
    dataset_configs: Mapping[str, CPGDatasetConfig],
) -> list[str]:
    """The sorted union of every Project's datasets.

    Raises:
        ValueError: if a Project lists a dataset that is not configured.
    """
    for project_id, project in projects.items():
        for dataset in project.datasets:
            if dataset not in dataset_configs:
                raise ValueError(
                    f'Themis project {project_id!r} lists dataset {dataset!r}, '
                    'which is not configured',
                )
    return sorted({d for p in projects.values() for d in p.datasets})


def _member_email(
    member_key: MemberKey,
    users: Mapping[MemberKey, CPGInfrastructureUser],
    *,
    context: str,
) -> str:
    user = users.get(member_key)
    if user is None:
        raise ValueError(f'Themis {context}: member {member_key!r} not found in config')
    cloud_user = user.clouds.get(GcpInfrastructure.name())
    if cloud_user is None:
        raise ValueError(
            f'Themis {context}: member {member_key!r} has no Google account '
            '(`gcp.id`) in the users config',
        )
    if '@' not in cloud_user.id:
        raise ValueError(
            f'Themis {context}: member {member_key!r} has `gcp.id` {cloud_user.id!r}, '
            'which is not the email of a Google account',
        )
    return cloud_user.id.lower()


def build_acls(
    themis: CPGInfrastructureConfig.Themis,
    projects: Mapping[str, CPGInfrastructureConfig.Themis.Project],
    users: Mapping[MemberKey, CPGInfrastructureUser],
    dataset_configs: Mapping[str, CPGDatasetConfig],
) -> dict[str, Any]:
    """Resolve one environment's ACLs: member keys become lowercase emails.

    `admitted` is the union of the environment's operators and every Project's
    members, so each Project member is also admitted. With no Projects, the
    ACL file admits the operators alone, which Themis accepts.

    Raises:
        ValueError: on an unknown member key, a member without a Google account, or
            a dataset that is not configured.
    """
    themis_datasets(projects, dataset_configs)

    acl_projects = {
        project_id: {
            'name': project.name,
            'datasets': sorted(project.datasets),
            'members': sorted(
                {
                    _member_email(m, users, context=f'project {project_id!r}')
                    for m in project.members
                },
            ),
        }
        for project_id, project in projects.items()
    }
    operators = {_member_email(m, users, context='operators') for m in themis.operators}
    admitted = operators.union(*(p['members'] for p in acl_projects.values()))

    return {
        'format': ACLS_FORMAT,
        'admitted': sorted(admitted),
        'projects': acl_projects,
    }


def serialise_acls(acls: dict[str, Any]) -> str:
    """Serialise deterministically, so an unchanged config writes no new object."""
    return json.dumps(acls, indent=2, sort_keys=True) + '\n'
