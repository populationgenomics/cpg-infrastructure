"""
Resolve the Themis roster from config.

Themis reads one JSON object, the roster, and nothing else from this
infrastructure. Themis owns its format: it rejects a roster whose `format` it
does not know, whose emails are not lowercase, or that lists a Project member
missing from `admitted`. `CPGInfrastructure.setup_themis` writes the result.
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

ROSTER_FORMAT = 1
ROSTER_OBJECT_NAME = 'roster.json'


def themis_datasets(
    themis: CPGInfrastructureConfig.Themis,
    dataset_configs: Mapping[str, CPGDatasetConfig],
) -> list[str]:
    """The sorted union of every Project's datasets.

    Raises:
        ValueError: if a Project lists a dataset that is not configured, or one
            that does not set `themis_enabled`.
    """
    for project_id, project in themis.projects.items():
        for dataset in project.datasets:
            dataset_config = dataset_configs.get(dataset)
            if dataset_config is None:
                raise ValueError(
                    f'Themis project {project_id!r} lists dataset {dataset!r}, '
                    'which is not configured',
                )
            if not dataset_config.themis_enabled:
                raise ValueError(
                    f'Themis project {project_id!r} lists dataset {dataset!r}, '
                    'which does not set themis_enabled',
                )
    return sorted({d for p in themis.projects.values() for d in p.datasets})


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
            f'Themis {context}: member {member_key!r} does not have a gcp id specified',
        )
    if '@' not in cloud_user.id:
        raise ValueError(
            f'Themis {context}: member {member_key!r} has gcp id {cloud_user.id!r}, '
            'which is not an email address',
        )
    return cloud_user.id.lower()


def build_roster(
    themis: CPGInfrastructureConfig.Themis,
    users: Mapping[MemberKey, CPGInfrastructureUser],
    dataset_configs: Mapping[str, CPGDatasetConfig],
) -> dict[str, Any]:
    """Resolve the roster: member keys become lowercase emails.

    `admitted` is the union of the operators and every Project's members, so
    each Project member is also admitted.

    Raises:
        ValueError: on an unknown member key, a member without a gcp email, or
            a dataset rejected by `themis_datasets`.
    """
    themis_datasets(themis, dataset_configs)

    projects = {
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
        for project_id, project in themis.projects.items()
    }
    operators = {_member_email(m, users, context='operators') for m in themis.operators}
    admitted = operators.union(*(p['members'] for p in projects.values()))

    return {
        'format': ROSTER_FORMAT,
        'admitted': sorted(admitted),
        'projects': projects,
    }


def serialise_roster(roster: dict[str, Any]) -> str:
    """Serialise deterministically, so an unchanged config writes no new object."""
    return json.dumps(roster, indent=2, sort_keys=True) + '\n'
