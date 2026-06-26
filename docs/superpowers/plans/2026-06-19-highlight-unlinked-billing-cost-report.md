# Highlight Projects with Unlinked Billing in Cost Report — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface GCP projects whose billing account has been unlinked/disabled at the top of the daily cost-report Slack message, so they aren't forgotten and re-linked late (data-deletion risk).

**Architecture:** Add a small, side-effect-free helper module (`billing_status.py`) inside the `gcp_cost_report_slack_bot` Cloud Function folder. It (1) decides which candidate projects have billing disabled via `google-cloud-billing`'s `CloudBillingClient`, and (2) rewrites those projects' rows in the existing `project_summary` so they render at the top of the flagged list with a `💵⛓️‍💥 Billing unlinked` indicator in the day column. `main.py` wires it in. The sibling `gcp_cost_control` function is left completely untouched.

**Tech Stack:** Python 3.11, `google-cloud-billing` (`CloudBillingClient`), `google.api_core.exceptions`, pytest (via `coverage run -m pytest test`), ruff, GCP Cloud Functions (gen2), Slack WebClient.

## Global Constraints

- Python **3.11**; runtime `python311`.
- Lint is ruff (pre-commit). Copy verbatim conventions from the repo: **single quotes**, **trailing commas in multi-line literals/calls**, **type annotations on new functions** (ANN101/ANN201/ANN401 are ignored, others enforced), **no blind `except Exception`** (BLE001 is enabled — catch specific exceptions). After editing, run `ruff check --fix` to settle import ordering (`I` is fixable).
- **Do NOT modify `cpg_infra/billing_aggregator/gcp_cost_control/`** in any way. Its `is_billing_enabled` stays as-is; we are NOT sharing code across the two functions.
- **Do NOT modify `driver.py` (Pulumi)** — production deployment and the daily `0 9 * * *` schedule stay as they are.
- `main.py` must remain deployable as a flat Cloud Function: it imports sibling modules flatly (`from billing_status import ...`), matching its existing `import slack` / `import functions_framework` style.
- The feature is **read-only** on GCP: `get_project_billing_info`, `list_budgets`, BigQuery reads, Slack posts only. It must never disable/enable billing.
- Indicator string is exactly `💵⛓️‍💥 Billing unlinked`. No-cost unlinked rows show `No monthly cost` in the month column.
- Candidate project set = **union** of budget-map project ids and cost-report project ids, excluding the `<none>` bucket.
- Error handling is **three-state**: flag a project ONLY on a definitive `billing_enabled == False`; on any exception, skip + log warning (never a false alert).

---

## File Structure

- **Create** `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/billing_status.py`
  Pure, importable, no module-level network calls. Holds `UNLINKED_DAY_LABEL`, `UNLINKED_SORT_TIER`, `get_unlinked_project_ids(...)`, `apply_unlinked_to_summary(...)`.
- **Create** `test/test_billing_status.py`
  Unit tests for both functions (client + summary are injected → fully mockable, no GCP/network).
- **Modify** `requirements-dev.txt`
  Add `google-cloud-billing` so CI's test env can import `billing_status` (CI installs `requirements-dev.txt`, not the function's own `requirements.txt`).
- **Modify** `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/requirements.txt`
  Add `google-cloud-billing` so the deployed function has the client lib.
- **Modify** `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/main.py`
  Instantiate `CloudBillingClient`, build the candidate union, call `get_unlinked_project_ids`, then `apply_unlinked_to_summary` — placed after the `len(totals) == 0` guard.
- **Modify** `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/deploy.sh`
  Switch the manual test deploy from `--trigger-topic` to `--trigger-http`; `cd` into the function folder so source is correct.
- **Modify** `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/README.md`
  Document the manual deploy + `curl` test loop against `#sabrina-dev`.

**Why a separate module instead of editing `main.py` directly:** `main.py` runs network calls at import time (`google.auth.default()`, `secret_manager.access_secret_version(...)`), so it cannot be imported in unit tests without GCP credentials. Putting the new logic in `billing_status.py` (which only imports libraries, never constructs clients at module scope) makes it unit-testable. `main.py` integration is verified by manual human testing in Task 6.

---

### Task 1: `get_unlinked_project_ids` helper (billing check + three-state error handling)

**Files:**
- Create: `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/billing_status.py`
- Test: `test/test_billing_status.py`
- Modify: `requirements-dev.txt` (add `google-cloud-billing`)

**Interfaces:**
- Consumes: nothing from earlier tasks. A `billing_client` is injected by the caller and must expose `get_project_billing_info(name="projects/<id>") -> object with .billing_enabled: bool`, raising `google.api_core.exceptions` subclasses on error.
- Produces:
  - `UNLINKED_DAY_LABEL: str` == `'💵⛓️‍💥 Billing unlinked'`
  - `UNLINKED_SORT_TIER: int` == `2`
  - `get_unlinked_project_ids(project_ids: Iterable[str], billing_client: Any) -> set[str]`

- [ ] **Step 1: Add the test-env dependency**

Edit `requirements-dev.txt` — add one line after `google-cloud-storage==2.14.0`:

```
google-cloud-billing
```

Then install it into your project virtualenv (the same interpreter that runs pytest):

```bash
pip install google-cloud-billing
```

- [ ] **Step 2: Verify the `google-cloud-billing` API surface (guard against API drift)**

Run this in your venv and confirm it prints `billing_enabled True`:

```bash
python -c "from google.cloud.billing_v1 import CloudBillingClient; from google.cloud.billing_v1.types import ProjectBillingInfo; print('billing_enabled', ProjectBillingInfo(billing_enabled=True).billing_enabled); import google.api_core.exceptions as e; print('NotFound<=GoogleAPICallError', issubclass(e.NotFound, e.GoogleAPICallError))"
```

Expected output:
```
billing_enabled True
NotFound<=GoogleAPICallError True
```

If the attribute name differs in the installed version, adjust the implementation in Step 4 accordingly before proceeding.

- [ ] **Step 3: Write the failing test**

Create `test/test_billing_status.py`:

```python
"""Tests for the unlinked-billing helpers used by the cost-report Slack bot."""

from unittest import TestCase
from unittest.mock import MagicMock

from google.api_core.exceptions import NotFound

from cpg_infra.billing_aggregator.gcp_cost_report_slack_bot.billing_status import (
    get_unlinked_project_ids,
)


def _billing_client(states: dict[str, bool], errors: set[str]) -> MagicMock:
    """Build a fake CloudBillingClient.

    states maps project_id -> billing_enabled bool.
    errors is a set of project_ids that should raise on lookup.
    """

    def fake_get(name: str) -> MagicMock:
        project_id = name.split('/')[-1]
        if project_id in errors:
            raise NotFound(f'no such project {project_id}')
        info = MagicMock()
        info.billing_enabled = states[project_id]
        return info

    client = MagicMock()
    client.get_project_billing_info.side_effect = lambda name: fake_get(name)
    return client


class TestGetUnlinkedProjectIds(TestCase):
    """get_unlinked_project_ids returns only definitively-disabled projects."""

    def test_returns_only_disabled_projects(self):
        client = _billing_client(
            states={'enabled-a': True, 'disabled-b': False, 'enabled-c': True},
            errors=set(),
        )
        result = get_unlinked_project_ids(
            ['enabled-a', 'disabled-b', 'enabled-c'],
            client,
        )
        self.assertEqual({'disabled-b'}, result)

    def test_errors_are_skipped_not_flagged(self):
        """A lookup error must NOT produce a false 'unlinked' result."""
        client = _billing_client(
            states={'disabled-b': False},
            errors={'boom-x'},
        )
        result = get_unlinked_project_ids(['disabled-b', 'boom-x'], client)
        self.assertEqual({'disabled-b'}, result)

    def test_empty_input(self):
        client = _billing_client(states={}, errors=set())
        self.assertEqual(set(), get_unlinked_project_ids([], client))
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `python -m pytest test/test_billing_status.py -v`
Expected: FAIL — `ModuleNotFoundError` / `ImportError` for `billing_status` (module not created yet).

- [ ] **Step 5: Write the minimal implementation**

Create `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/billing_status.py`:

```python
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
    """
    unlinked: set[str] = set()
    for project_id in project_ids:
        try:
            info = billing_client.get_project_billing_info(
                name=f'projects/{project_id}',
            )
        except (GoogleAPICallError, RetryError, GoogleAuthError) as err:
            logging.warning(
                f'Could not determine billing status for {project_id}, '
                f'skipping: {err}',
            )
            continue
        if not info.billing_enabled:
            unlinked.add(project_id)
    return unlinked
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `python -m pytest test/test_billing_status.py -v`
Expected: PASS (3 passed).

- [ ] **Step 7: Lint the new files**

Run: `ruff check --fix cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/billing_status.py test/test_billing_status.py`
Expected: no remaining errors.

- [ ] **Step 8: Commit**

```bash
git add cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/billing_status.py test/test_billing_status.py requirements-dev.txt
git commit -m "feat(cost-report): add get_unlinked_project_ids billing check"
```

---

### Task 2: `apply_unlinked_to_summary` (rewrite rows + bump sort to top)

**Files:**
- Modify: `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/billing_status.py`
- Modify (add tests): `test/test_billing_status.py`

**Interfaces:**
- Consumes: `UNLINKED_DAY_LABEL`, `UNLINKED_SORT_TIER`, `_ROW_COLUMN_SEPARATOR`, `_NO_MONTH_PLACEHOLDER` from Task 1.
- Produces:
  - `apply_unlinked_to_summary(project_summary: dict[str, dict[str, Any]], unlinked_project_ids: Iterable[str], make_project_link: Callable[[str], str]) -> dict[str, dict[str, Any]]`
  - `project_summary` entry shape (matches `main.py`): `{project_id: {'sort': (tier_or_bool, day_total, month_total), 'value': (project_link, 'day_str | month_str')}}`. The function mutates in place and returns the same dict.

- [ ] **Step 1: Write the failing tests**

Append to `test/test_billing_status.py`:

```python
from cpg_infra.billing_aggregator.gcp_cost_report_slack_bot.billing_status import (
    UNLINKED_DAY_LABEL,
    UNLINKED_SORT_TIER,
    apply_unlinked_to_summary,
)


class TestApplyUnlinkedToSummary(TestCase):
    """apply_unlinked_to_summary marks unlinked projects and sorts them top."""

    def test_existing_project_day_column_overridden_month_kept(self):
        summary = {
            'proj-a': {
                'sort': (True, 5.0, 100.0),
                'value': ('<url|*proj-a*>', '$5 + $1 AUD (12%) | $100 AUD (50%)'),
            },
        }
        apply_unlinked_to_summary(summary, {'proj-a'}, make_project_link=str)

        link, row = summary['proj-a']['value']
        self.assertEqual('<url|*proj-a*>', link)
        self.assertEqual(
            f'{UNLINKED_DAY_LABEL} | $100 AUD (50%)',
            row,
        )
        # tier on top; month_total becomes the primary tie-break (desc).
        self.assertEqual((UNLINKED_SORT_TIER, 100.0, 5.0), summary['proj-a']['sort'])

    def test_budget_only_project_is_synthesised(self):
        summary: dict = {}
        apply_unlinked_to_summary(
            summary,
            {'proj-b'},
            make_project_link=lambda pid: f'<link|{pid}>',
        )
        self.assertIn('proj-b', summary)
        link, row = summary['proj-b']['value']
        self.assertEqual('<link|proj-b>', link)
        self.assertEqual(f'{UNLINKED_DAY_LABEL} | No monthly cost', row)
        self.assertEqual((UNLINKED_SORT_TIER, 0.0, 0.0), summary['proj-b']['sort'])

    def test_non_unlinked_projects_untouched(self):
        summary = {
            'proj-c': {
                'sort': (False, 1.0, 2.0),
                'value': ('<url|proj-c>', '$1 | $2'),
            },
        }
        apply_unlinked_to_summary(summary, set(), make_project_link=str)
        self.assertEqual((False, 1.0, 2.0), summary['proj-c']['sort'])
        self.assertEqual(('<url|proj-c>', '$1 | $2'), summary['proj-c']['value'])
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest test/test_billing_status.py::TestApplyUnlinkedToSummary -v`
Expected: FAIL — `ImportError: cannot import name 'apply_unlinked_to_summary'`.

- [ ] **Step 3: Write the minimal implementation**

Append to `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/billing_status.py`:

```python
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
```

- [ ] **Step 4: Run the full test file to verify all pass**

Run: `python -m pytest test/test_billing_status.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Lint**

Run: `ruff check --fix cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/billing_status.py test/test_billing_status.py`
Expected: no remaining errors.

- [ ] **Step 6: Commit**

```bash
git add cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/billing_status.py test/test_billing_status.py
git commit -m "feat(cost-report): add apply_unlinked_to_summary row rewriter"
```

---

### Task 3: Wire the helpers into `main.py`

**Files:**
- Modify: `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/main.py`
- Modify: `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/requirements.txt`

**Interfaces:**
- Consumes: `get_unlinked_project_ids`, `apply_unlinked_to_summary` from Tasks 1–2.
- Consumes (existing in `main.py`): `get_budget_map()` (keys are project ids), `project_summary` (built in `slack_bot_cost_report`), `billing_link(project_id) -> str`.
- Produces: no new public surface — this is integration. Behavior: unlinked projects appear at the top of the flagged list every run.

> **Note:** `main.py` is not unit-tested (it performs network calls at import time). This task is verified by `py_compile` + `ruff` here, and end-to-end by Task 5's manual human test.

- [ ] **Step 1: Add the deployed-function dependency**

Edit `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/requirements.txt` — add one line:

```
google-cloud-billing
```

(Full file becomes:)
```
google-cloud-bigquery
google-cloud-secret-manager
google-cloud-billing-budgets
google-cloud-billing
flask<3.0,>=1.0
functions-framework
slackclient
pytz
```

- [ ] **Step 2: Add imports to `main.py`**

In `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/main.py`, add the client import next to the existing google imports (after line `from google.cloud.billing import budgets_v1 as budget`):

```python
from google.cloud.billing_v1 import CloudBillingClient
```

And add the sibling-module import (flat, matching the function's layout). Place it after the third-party imports (e.g. after `from slack.errors import SlackApiError`):

```python
from billing_status import apply_unlinked_to_summary, get_unlinked_project_ids
```

- [ ] **Step 3: Instantiate the billing client**

Find these lines (~line 119-120):

```python
bigquery_client = bigquery.Client()
budget_client = budget.BudgetServiceClient()
```

Add immediately below:

```python
cloud_billing_client = CloudBillingClient()
```

- [ ] **Step 4: Insert the unlinked-billing pass in `slack_bot_cost_report`**

Find the `len(totals) == 0` guard inside `slack_bot_cost_report` (~lines 347-351):

```python
    if len(totals) == 0:
        logging.info(
            "No information to log, this function won't log anything to slack.",
        )
        return 'Nothing to log', 204
```

Immediately **after** that block (before the `# Format the totals` comment), insert:

```python
    # Flag projects whose billing account has been unlinked/disabled so they
    # appear at the top of the flagged list until billing is re-linked. The
    # candidate set is the union of every project with a budget and every
    # project in the cost report (excluding the '<none>' bucket).
    candidate_project_ids = (
        set(get_budget_map().keys()) | set(project_summary.keys())
    ) - {'<none>'}
    unlinked_project_ids = get_unlinked_project_ids(
        candidate_project_ids,
        cloud_billing_client,
    )
    if unlinked_project_ids:
        logging.info(
            f'Projects with unlinked billing: {sorted(unlinked_project_ids)}',
        )
    apply_unlinked_to_summary(
        project_summary,
        unlinked_project_ids,
        make_project_link=lambda pid: f'<{billing_link(pid)}|{pid}>',
    )
```

> Placed after the `len(totals) == 0` guard so we make no billing API calls on days with nothing to post. `apply_unlinked_to_summary` mutates `project_summary` in place; the existing `post_slack_message` then buckets sort-tier `2` rows into the flagged list and sorts them to the top.

- [ ] **Step 5: Byte-compile to catch syntax/indentation errors**

Run: `python -m py_compile cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/main.py`
Expected: no output, exit code 0.

- [ ] **Step 6: Lint `main.py`**

Run: `ruff check --fix cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/main.py`
Expected: no remaining errors. (Confirm the `from billing_status import ...` line ends up correctly ordered; ruff's `I` autofix handles this.)

- [ ] **Step 7: Confirm the unit suite is still green**

Run: `python -m pytest test -v`
Expected: PASS (existing `test_parse_values.py` + 6 billing-status tests).

- [ ] **Step 8: Commit**

```bash
git add cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/main.py cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/requirements.txt
git commit -m "feat(cost-report): surface unlinked-billing projects in daily message"
```

---

### Task 4: Make `deploy.sh` HTTP-triggered + document the test loop

**Files:**
- Modify: `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/deploy.sh`
- Modify: `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/README.md`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: a manual test-deploy workflow targeting `#sabrina-dev`, triggerable on demand via `curl` (supporting `{"force_run": true}`).

- [ ] **Step 1: Update `deploy.sh` to an HTTP trigger**

Replace the entire contents of `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/deploy.sh` with:

```bash
#!/bin/bash
# Manual TEST deploy of the cost-report Slack bot.
# Posts to the dev Slack channel below — NOT production. Production is managed
# by Pulumi (cpg_infra/billing_aggregator/driver.py); do not use this for prod.
set -euo pipefail

# Always deploy this folder regardless of where the script is invoked from.
cd "$(dirname "$0")"

BILLING_ADMIN_PROJECT="billing-admin-290403"
REGION="australia-southeast1"
SERVICE_ACCOUNT="gcp-cost-control@billing-admin-290403.iam.gserviceaccount.com"
SLACK_CHANNEL="sabrina-dev"
BIGQUERY_BILLING_TABLE="billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343"
QUERY_TIME_ZONE="Australia/Sydney"
BILLING_ACCOUNT_ID="01D012-20A6A2-CBD343"

gcloud config set project $BILLING_ADMIN_PROJECT
gcloud functions deploy slack_bot_cost_report --runtime python311 \
    --gen2 \
    --region=$REGION \
    --service-account $SERVICE_ACCOUNT \
    --set-env-vars SLACK_CHANNEL=$SLACK_CHANNEL \
    --set-env-vars BIGQUERY_BILLING_TABLE=$BIGQUERY_BILLING_TABLE \
    --set-env-vars QUERY_TIME_ZONE=$QUERY_TIME_ZONE \
    --set-env-vars BILLING_ACCOUNT_ID=$BILLING_ACCOUNT_ID \
    --trigger-http \
    --no-allow-unauthenticated \
    --timeout=540s
```

- [ ] **Step 2: Document the test loop in `README.md`**

Append the following section to `cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/README.md`:

```markdown
## Manual testing (dev)

Production is deployed by Pulumi (`cpg_infra/billing_aggregator/driver.py`) on a
daily `0 9 * * *` schedule and posts to `#production-announcements`. For
iterating on changes, deploy a separate HTTP-triggered copy that posts to
`#sabrina-dev` and trigger it on demand. The function is read-only on GCP
(reads BigQuery, budgets and project billing info; posts to Slack) — it cannot
change any project's billing.

```bash
# 1. Deploy the test function (posts to the SLACK_CHANNEL set in deploy.sh).
./deploy.sh

# 2. Grab its URL.
URL=$(gcloud functions describe slack_bot_cost_report \
    --region=australia-southeast1 --gen2 \
    --format='value(serviceConfig.uri)')

# 3a. Daily-style run: flagged projects + any unlinked-billing projects.
curl -m 600 -X POST "$URL" \
    -H "Authorization: bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d '{}'

# 3b. Full dump (everything, as on Mondays): add force_run.
curl -m 600 -X POST "$URL" \
    -H "Authorization: bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d '{"force_run": true}'

# 4. (Optional) tear down when finished.
gcloud functions delete slack_bot_cost_report \
    --region=australia-southeast1 --gen2 --quiet
```

If `curl` returns 403, grant yourself the invoker role on the function
(`roles/run.invoker` for gen2) and retry.
```

- [ ] **Step 3: Lint the shell script (best-effort) and commit**

```bash
chmod +x cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/deploy.sh
git add cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/deploy.sh cpg_infra/billing_aggregator/gcp_cost_report_slack_bot/README.md
git commit -m "chore(cost-report): http-trigger test deploy + document test loop"
```

---

### Task 5: Manual human end-to-end test against `#sabrina-dev`

**Files:** none (verification only).

**Interfaces:**
- Consumes: the deployed test function from Task 4 and all logic from Tasks 1–3.
- Produces: confirmation the feature works live and prod is untouched.

> This is the human-in-the-loop test the unit tests can't cover (live GCP + Slack rendering). Rendering correctness is already proven by Tasks 1–2; this confirms wiring, permissions, and visual output.

- [ ] **Step 1: Preconditions**

Confirm you're on branch `SET-1051-highlight-projects-with-unlinked-billing-in-cost-report-slack-message`, deps installed, and `deploy.sh` has `SLACK_CHANNEL="sabrina-dev"` (NOT a prod channel).

- [ ] **Step 2: Deploy + trigger the daily-style run**

```bash
cd cpg_infra/billing_aggregator/gcp_cost_report_slack_bot
./deploy.sh
# then run steps 2 & 3a from the README test loop
```
Expected: HTTP 200; a cost-report message appears in `#sabrina-dev`; no errors.

- [ ] **Step 3: Verify the unlinked rendering**

In `#sabrina-dev`, confirm:
- If any project currently has unlinked billing, it appears at the **top of the flagged list**, with `💵⛓️‍💥 Billing unlinked` in the left (Previous Day) column and its normal monthly value (or `No monthly cost`) in the right column.
- No healthy project is falsely labelled unlinked.

If **no** project is currently unlinked (so the indicator can't be seen), deliberately exercise the path: temporarily **unlink billing on a disposable sandbox project you own**, re-run the curl, confirm it shows at the top with the indicator, then **re-link billing** (the function is read-only and will NOT re-link it for you).

- [ ] **Step 4: Verify the full-dump path**

Run README step 3b (`force_run`). Expected: the full multi-chunk project list posts to `#sabrina-dev`, with unlinked projects still at the top of the flagged chunk.

- [ ] **Step 5: Check logs + confirm prod untouched**

```bash
gcloud functions logs read slack_bot_cost_report --region=australia-southeast1 --gen2 --limit=50
```
Expected: an info line `Projects with unlinked billing: [...]` when applicable; no `Could not determine billing status` warnings for healthy projects. Confirm `#production-announcements` and the Pulumi-managed function/schedule were not affected.

- [ ] **Step 6: (Optional) tear down the test function**

Run README step 4 to delete the test function when finished.

---

## Self-Review

**1. Spec coverage:**
- "Add projects without a billing account to the daily message" → Tasks 1–3 (computation + render on the daily/flagged path). ✅
- Use `gcp_cost_control` logic, possibly duplicate, refactor "not at the expense of breaking it" → decided: no shared module, `gcp_cost_control` untouched; comment in `billing_status.py` references the sibling. ✅ (Global Constraints)
- Source set = union of budget-map ∪ cost-report projects → Task 3 Step 4. ✅
- Use `google-cloud-billing` `CloudBillingClient` → Tasks 1 & 3. ✅
- Three-state error handling (only flag definitive `False`) → Task 1 Step 5 + test `test_errors_are_skipped_not_flagged`. ✅
- Render: prepend to top of flagged list, `💵⛓️‍💥 Billing unlinked` in day column, month column unchanged / `No monthly cost` → Task 2 + tests. ✅
- Sustainable deploy/test via manual deploy to `#sabrina-dev`, HTTP trigger + curl, documented → Tasks 4 & 5. ✅
- "Cost reporting bot updated and deployed" (DoD) → Task 5 (deploy + verify). ✅

**2. Placeholder scan:** No TBD/“handle edge cases”/“similar to Task N”. All code blocks are complete; all commands have expected output. ✅

**3. Type consistency:** `get_unlinked_project_ids(project_ids, billing_client) -> set[str]` and `apply_unlinked_to_summary(project_summary, unlinked_project_ids, make_project_link) -> dict` are used with identical names/shapes in Task 3. `UNLINKED_DAY_LABEL` / `UNLINKED_SORT_TIER` constants are defined in Task 1 and consumed in Task 2 tests + implementation. `project_summary` entry shape (`{'sort': (...), 'value': (link, row)}`) matches `main.py` lines 342-345. ✅
```
