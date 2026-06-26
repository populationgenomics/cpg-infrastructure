"""Helpers for flagging GCP projects with unlinked (disabled) billing.

These live alongside the cost-report Cloud Function so they are bundled into
its source archive (see billing_aggregator/driver.py::create_source_archive).

The sibling `gcp_cost_control` function has its own `is_billing_enabled`
implementation (built on the legacy discovery API). It is intentionally NOT
shared from here, so that this change cannot affect that production function.
"""

import logging
from collections.abc import Callable, Iterable
from typing import Any

from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.auth.exceptions import GoogleAuthError

# Shown in the "Previous Day" column for a project whose billing is unlinked.
UNLINKED_DAY_LABEL = '💵⛓️‍💥 Billing unlinked'

# Sort tier that places unlinked projects above ordinary flagged projects.
# The existing sort key's first element is a bool (flagged == True == 1);
# 2 sorts above that under the report's reverse=True ordering.
UNLINKED_SORT_TIER = 2

_ROW_COLUMN_SEPARATOR = ' | '
_NO_MONTH_PLACEHOLDER = 'No monthly cost'


def get_unlinked_project_ids(
    project_ids: Iterable[str],
    billing_client: Any,
) -> set[str]:
    """Return the subset of project_ids whose billing is definitively disabled.

    Uses CloudBillingClient.get_project_billing_info. A project is included
    ONLY when the API returns billing_enabled == False. Any error (not found,
    permission denied, transport/auth failure) is treated as "unknown": the
    project is skipped and logged, so a transient failure never produces a
    false "billing unlinked" alert.

    Skips are logged at INFO, not WARNING: the candidate set includes external
    projects the service account has no IAM on, which 403 on every run, so these
    skips are expected and non-actionable. (The choice doesn't affect alerting:
    this function doesn't configure structured logging, so its app logs reach
    Cloud Logging at DEFAULT severity regardless of Python level. INFO is purely
    for readability.)
    """
    unlinked: set[str] = set()
    for project_id in project_ids:
        try:
            info = billing_client.get_project_billing_info(
                name=f'projects/{project_id}',
            )
        except (GoogleAPICallError, RetryError, GoogleAuthError) as err:
            logging.info(
                f'Could not determine billing status for {project_id}, '
                f'skipping: {err}',
            )
            continue
        if not info.billing_enabled:
            unlinked.add(project_id)
    return unlinked


def apply_unlinked_to_summary(
    project_summary: dict[str, dict[str, Any]],
    unlinked_project_ids: Iterable[str],
    make_project_link: Callable[[str], str],
) -> dict[str, dict[str, Any]]:
    """Mark unlinked projects in the cost-report project summary.

    For every unlinked project:
      * the "Previous Day" column is replaced with UNLINKED_DAY_LABEL,
      * the "Month (%)" column is preserved (or set to "No monthly cost" when
        the project has no cost row this run),
      * the sort key is bumped to UNLINKED_SORT_TIER so the project sorts to
        the very top of the flagged list, ordered by monthly spend desc.

    project_summary is mutated in place and also returned for convenience.
    Entries are shaped like:
        {project_id: {'sort': (tier_or_bool, day_total, month_total),
                      'value': (project_link, 'day_str | month_str')}}
    """
    for project_id in unlinked_project_ids:
        existing = project_summary.get(project_id)
        if existing is not None:
            project_link, row_str = existing['value']
            if _ROW_COLUMN_SEPARATOR in row_str:
                month_str = row_str.split(_ROW_COLUMN_SEPARATOR, 1)[1]
            else:
                month_str = _NO_MONTH_PLACEHOLDER
            old_sort = existing['sort']
            existing['value'] = (
                project_link,
                f'{UNLINKED_DAY_LABEL}{_ROW_COLUMN_SEPARATOR}{month_str}',
            )
            # (tier, month_total, day_total): unlinked rows order by month desc.
            existing['sort'] = (UNLINKED_SORT_TIER, old_sort[2], old_sort[1])
        else:
            project_summary[project_id] = {
                'sort': (UNLINKED_SORT_TIER, 0.0, 0.0),
                'value': (
                    make_project_link(project_id),
                    f'{UNLINKED_DAY_LABEL}{_ROW_COLUMN_SEPARATOR}'
                    f'{_NO_MONTH_PLACEHOLDER}',
                ),
            }
    return project_summary
