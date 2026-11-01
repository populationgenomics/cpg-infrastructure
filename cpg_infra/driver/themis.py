"""
Build Themis's ACL, which maps each Themis Project (a dataset) to the emails of
its managers and curators.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.driver.constants import THEMIS_CURATOR, THEMIS_MANAGER

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from cpg_infra.config import (
        CPGDatasetConfig,
        CPGInfrastructureUser,
        GroupName,
        MemberKey,
    )

ACL_FORMAT = 1
ROLE_LISTS: dict[str, GroupName] = {
    'managers': THEMIS_MANAGER,
    'curators': THEMIS_CURATOR,
}


def _member_email(
    member_key: MemberKey,
    users: Mapping[MemberKey, CPGInfrastructureUser],
    *,
    context: str,
) -> str:
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
            'Themis roles are for people',
        )
    return email.lower()


def build_acl(
    allowed_datasets: Iterable[str],
    dataset_configs: Mapping[str, CPGDatasetConfig],
    users: Mapping[MemberKey, CPGInfrastructureUser],
) -> dict[str, Any]:
    """One environment's ACL.

    Each allowed dataset with a non-empty themis-manager or themis-curator list
    becomes a Project, with both roles as sorted lowercase emails.
    """
    projects = {}
    for dataset in allowed_datasets:
        dataset_config = dataset_configs.get(dataset)
        if dataset_config is None:
            raise ValueError(
                f'Themis allows dataset {dataset!r}, which is not configured',
            )
        roles = {
            role: sorted(
                {
                    _member_email(m, users, context=f'Themis: {dataset} {list_name}')
                    for m in dataset_config.members.get(list_name, [])
                },
            )
            for role, list_name in ROLE_LISTS.items()
        }
        if any(roles.values()):
            projects[dataset] = roles

    return {'format': ACL_FORMAT, 'projects': projects}


def serialise_acl(acl: dict[str, Any]) -> str:
    """Compact, deterministic JSON, so an unchanged ACL adds no secret version."""
    return json.dumps(acl, sort_keys=True, separators=(',', ':'))
