"""
Resolve the Themis ACL file from config.

Themis reads one JSON object, the ACL file, and nothing else from this
infrastructure. It holds Themis's access control lists: `projects` maps each
Project's id to its members' emails. A Themis Project is one dataset, and its
id is the dataset's name. Themis owns the format: it rejects a file whose
`format` it does not know, that has keys it does not know, or whose emails are
not lowercase. `CPGInfrastructure.setup_themis` writes the result.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.driver.constants import THEMIS_ACCESS

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from cpg_infra.config import (
        CPGDatasetConfig,
        CPGInfrastructureUser,
        MemberKey,
    )

ACLS_FORMAT = 1
ACLS_OBJECT_NAME = 'acls.json'


def _member_email(
    member_key: MemberKey,
    users: Mapping[MemberKey, CPGInfrastructureUser],
    *,
    dataset: str,
) -> str:
    context = f'Themis: {dataset} {THEMIS_ACCESS}'
    user = users.get(member_key)
    if user is None:
        raise ValueError(f'{context}: member {member_key!r} not found in config')
    cloud_user = user.clouds.get(GcpInfrastructure.name())
    if cloud_user is None:
        raise ValueError(
            f'{context}: member {member_key!r} has no Google account '
            '(`gcp.id`) in the users config',
        )
    email = cloud_user.id
    if '@' not in email or not email.isascii():
        raise ValueError(
            f'{context}: member {member_key!r} has `gcp.id` {email!r}, '
            'which is not the email of a Google account',
        )
    if email.lower().endswith('.gserviceaccount.com'):
        raise ValueError(
            f'{context}: member {member_key!r} is the service account {email!r}; '
            f'{THEMIS_ACCESS} lists people',
        )
    # ASCII only, so this is the ASCII lowercase Themis compares against
    return email.lower()


def build_acls(
    allowed_datasets: Iterable[str],
    dataset_configs: Mapping[str, CPGDatasetConfig],
    users: Mapping[MemberKey, CPGInfrastructureUser],
) -> dict[str, Any]:
    """Resolve one environment's ACL file.

    Each allowed dataset with a non-empty `themis-access` list becomes a
    Project: the dataset's name, mapped to those users' sorted lowercase
    emails. With no such dataset, the ACL file has no Projects, which
    Themis accepts.

    Raises:
        ValueError: on an allowed dataset that is not configured, an unknown
            member key, a member without the email of a Google account, or a
            service account.
    """
    projects = {}
    for dataset in allowed_datasets:
        dataset_config = dataset_configs.get(dataset)
        if dataset_config is None:
            raise ValueError(
                f'Themis allows dataset {dataset!r}, which is not configured',
            )
        if member_keys := dataset_config.members.get(THEMIS_ACCESS):
            projects[dataset] = sorted(
                {_member_email(m, users, dataset=dataset) for m in member_keys},
            )

    return {'format': ACLS_FORMAT, 'projects': projects}


def serialise_acls(acls: dict[str, Any]) -> str:
    """Serialise deterministically, so an unchanged config writes no new object."""
    return json.dumps(acls, indent=2, sort_keys=True) + '\n'
