# pylint: disable=too-many-locals
"""A Cloud Function to send a daily GCP cost report to Slack."""

import calendar
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from math import ceil
from typing import Any, Generator, Tuple

import flask
import functions_framework
import slack
from google.auth import default
from google.cloud import bigquery, secretmanager
from google.cloud.billing import budgets_v1 as budget
from pytz import timezone
from slack.errors import SlackApiError

# Custom types
SortKey = Tuple[float, float, float]

# Environment variables
BILLING_URL = 'https://sample-metadata.populationgenomics.org.au/billing/costByTime?groupBy=gcp_project'

BIGQUERY_BILLING_TABLE = os.getenv('BIGQUERY_BILLING_TABLE')
QUERY_TIME_ZONE = os.getenv('QUERY_TIME_ZONE') or 'UTC'
SLACK_MESSAGE_MAX_CHARS = 2000
FLAGGED_PROJECT_THRESHOLD = 0.8
HUNDREDS_ROUNDING_THRESHOLD = 100
TINY_VALUE_THRESHOLD = 0.01
TIMEZONE = timezone('Australia/Sydney')

# Query monthly cost per project and join that with cost over the last day.
BIGQUERY_QUERY = f"""
SELECT
  month.id as project_id,
  month.cost as month,
  day.cost as day,
  month.currency as currency,
  month.cost_category as cost_category
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          project.id,
          SUM(cost) as cost,
          currency,
          (CASE
            WHEN service.description='Cloud Storage' THEN 'Storage Cost'
            ELSE 'Compute Cost'
            END) as cost_category
        FROM
          `{BIGQUERY_BILLING_TABLE}`
        WHERE
          _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 32 DAY)
          AND invoice.month = FORMAT_TIMESTAMP("%Y%m", CURRENT_TIMESTAMP(),
            "{QUERY_TIME_ZONE}")
        GROUP BY
          project.id,
          currency,
          cost_category
      )
    WHERE
      cost > 0.1
  ) month
  LEFT JOIN (
    SELECT
      project.id,
      SUM(cost) as cost,
      currency,
      (CASE
        WHEN service.description='Cloud Storage' THEN 'Storage Cost'
        ELSE 'Compute Cost'
        END) as cost_category
    FROM
      `{BIGQUERY_BILLING_TABLE}`
    WHERE
      _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
      AND export_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
      AND invoice.month = FORMAT_TIMESTAMP("%Y%m", CURRENT_TIMESTAMP(),
        "{QUERY_TIME_ZONE}")
    GROUP BY
      project.id,
      currency,
      cost_category
  ) day ON month.id = day.id AND month.currency = day.currency
  AND month.cost_category = day.cost_category
ORDER BY
  day DESC;
"""

# Get credentials
CREDENTIALS, PROJECT_ID = default()

# Get slack token from secrets
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL')
SLACK_TOKEN_SECRET_NAME = (
    f'projects/{PROJECT_ID}/secrets/slack-gcp-cost-control/versions/latest'
)
BILLING_ACCOUNT_ID = os.getenv('BILLING_ACCOUNT_ID')

# Cache the Slack client.
secret_manager = secretmanager.SecretManagerServiceClient()
slack_token_response = secret_manager.access_secret_version(
    request={'name': SLACK_TOKEN_SECRET_NAME},
)
slack_token = slack_token_response.payload.data.decode('UTF-8')
slack_client = slack.WebClient(token=slack_token)

bigquery_client = bigquery.Client()
budget_client = budget.BudgetServiceClient()


# Cache the budgets for the billing account.
@lru_cache
def get_budget_map():
    budgets = budget_client.list_budgets(parent=f'billingAccounts/{BILLING_ACCOUNT_ID}')
    return {b.display_name: b for b in budgets}


def try_cast_int(i: Any) -> int | None:
    """Cast i to int, else return None if ValueError"""
    try:
        return int(i)
    except ValueError:
        return None


def try_cast_float(f: Any) -> float | None:
    """Cast i to float, else return None if ValueError"""
    try:
        return float(f)
    except ValueError:
        return None


def month_progress() -> float:
    """Return the percentage we are through the month"""
    today = datetime.now(tz=TIMEZONE).date()
    monthrange = calendar.monthrange(today.year, today.month)[1]
    return today.day / monthrange


def billing_link(project_id: str) -> str:
    return BILLING_URL + f'&selectedData={project_id}'


def format_billing_row(
    project_id: str | None,
    fields: dict,
    currency: str,
    percent_threshold: float = 0,
) -> tuple[SortKey, str | None, str, str]:
    """
    Formats the billing row for a project.

    Args:
        project_id (str | None): The ID of the project.
        fields (dict): The billing data for the project.
        currency (str): The currency used for formatting.
        percent_threshold (float, optional): The threshold for percent used. Defaults to 0.

    Returns:
        tuple[float, str, str, str]: The formatted billing row.

    """

    # Helper function to format money values
    def money_format(money: float | None) -> str | None:
        if money is None:
            return None
        if money < TINY_VALUE_THRESHOLD:
            return '<0.01'
        if money > HUNDREDS_ROUNDING_THRESHOLD:
            return f'{money:.0f}'
        return f'{money:.2f}'

    # Helper function to format cost categories
    def format_cost_categories(data: dict, currency: str) -> str:
        values = [
            f'{k.capitalize()[0]}: {money_format(data[k])}'
            for k in sorted(data.keys())
            if money_format(data[k]) is not None
        ]
        currency = ' ' + currency if 'AUD' not in currency else ''
        return ' '.join(values) + currency

    # Format cost categories for daily and monthly costs
    url = billing_link(project_id)
    project_link: str = project_id
    if url:
        project_link = f'<{url}|{project_id}>'

    row_str_1: str = format_cost_categories(fields['day'], currency)
    row_str_2: str = format_cost_categories(fields['month'], currency)

    # Get the budget for the project if available
    # Returns 0 and '' if no budget is available
    budget_map = get_budget_map()
    percent_used, percent_used_str = get_percent_used_from_budget(
        budget_map.get(project_id),
        fields['month']['total'],
        currency,
    )

    # Add percent used in brackets if available
    row_str_2 += f' ({percent_used_str})' if percent_used_str else ''

    # potential formatting
    if percent_used >= percent_threshold:
        # make fields bold
        project_link = f'*{project_link}*'
        if url:
            project_link = f'<{url}|*{project_id}*>'
        row_str_1 = f'*{row_str_1}*'
        row_str_2 = f'*{row_str_2}*'

    # The sort key is a tuple of the percent used, daily cost, and monthly cost
    # 0: percent used if above threshold, else 0
    # 1: sum of daily costs
    # 2: sum of monthly costs
    sort_key: SortKey = (
        percent_used if percent_used >= percent_threshold else 0,
        sum(x for x in fields['day'].values() if x),
        sum(x for x in fields['month'].values() if x),
    )

    # Placeholder string for no data
    row_str_1 = row_str_1 if row_str_1 else 'No daily cost'
    row_str_2 = row_str_2 if row_str_2 else 'No monthly cost'

    return sort_key, project_link, row_str_1, row_str_2


def num_chars(lst: list[str]) -> int:
    return len(''.join(lst))


@functions_framework.http
def slack_bot_cost_report(request: flask.Request):  # noqa: ARG001
    """
    Main entry point for the Cloud Function.

    This function generates a cost report for Google Cloud Platform (GCP) projects.
    It retrieves cost data from BigQuery and calculates the total cost for each project,
    broken down by currency and cost category. The function then formats the cost report
    and sends it as a message to Slack.
    """

    totals: dict[str, dict[str, dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(float)),
    )

    # Work out what percentage of the way we are through the month
    percent_threshold = month_progress()

    # Slack message header and summary
    project_summary: list[dict[str, Any]] = []
    totals_summary: list[tuple[str, str]] = []
    grouped_rows: dict[str, dict[str, dict[str, dict[str, float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(float))),
    )

    # Get the billing data from BigQuery and format it into grouped rows
    # keyed by project_id, currency, and day/month
    for row in bigquery_client.query(BIGQUERY_QUERY):
        project_id = row['project_id'] or '<none>'
        currency = row['currency']
        cost_category = row['cost_category']
        last_month = row['month']

        totals[currency][cost_category]['month'] += row['month']

        if row['day']:
            totals[currency][cost_category]['day'] += row['day']

        grouped_rows[project_id][currency]['day'][cost_category] = row['day']
        grouped_rows[project_id][currency]['month']['total'] += row['month']

    # Format the billing rows and add them to the project summary
    for project_id, by_currency in grouped_rows.items():
        for currency, row in by_currency.items():
            sort_key, prj_id, row_str_1, row_str_2 = format_billing_row(
                project_id,
                row,
                currency,
                percent_threshold,
            )
            project_summary.append(
                {'sort': sort_key, 'value': (prj_id, row_str_1 + ' / ' + row_str_2)},
            )

    if len(totals) == 0:
        logging.info(
            'No information to log, this function won\'t log anything to slack.',
        )
        return 'Nothing to log', 204

    # Format the totals and add them to the totals summary
    for currency, by_category in totals.items():
        fields: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        day_total: float = 0
        month_total: float = 0

        for cost_category, vals in by_category.items():
            last_day = vals['day']
            last_month = vals['month']
            day_total += last_day
            month_total += last_month
            fields['day'][cost_category] = last_day
            fields['month']['total'] += last_month

        # totals don't have percent used
        _, _, a, b = format_billing_row(None, fields, currency)
        totals_summary.append(
            (
                f'<{BILLING_URL}|*All projects:*>',
                a + ' / ' + b,
            ),
        )

        post_slack_message(project_summary, totals_summary, percent_threshold)

    return 'Success', 200


def post_slack_message(
    project_summary: list[dict],
    totals_summary: list[tuple[str, str]],
    percent_threshold: float = 0,
):
    flagged_projects_header = [
        '*Flagged Projects*',
        '*24h cost/Month cost (% used)*',
    ]
    normal_header = [
        '*Projects*',
        '*24h cost/Month cost (% used)*',
    ]
    flagged_projects_header_message = (
        f'We are {percent_threshold*100:.0f}% through the month. Costs exceeding '
        'the budget are flagged below:'
    )

    # Processing the projects summary into flagged and not flagged
    flagged_projects = [
        x['value']
        for x in sorted(project_summary, key=lambda x: x['sort'], reverse=True)
        if x['sort'][0]
    ]
    sorted_projects = [
        x['value']
        for x in sorted(project_summary, key=lambda x: x['sort'], reverse=True)
        if not x['sort'][0]
    ]

    all_rows = [*totals_summary, *sorted_projects]

    # If there are no flagged projects, exit without posting to slack
    is_monday = datetime.now(tz=TIMEZONE).weekday() == 0
    if not is_monday and len(flagged_projects) < 1:
        flagged_projects = [('No flagged projects', '-')]
        return  # Only post on Mondays or when a project gets flagged

    def chunk_list(lst: list, n: int) -> Generator[list, None, None]:
        n = max(n, 1)
        step = ceil(len(lst) / n)
        for i in range(0, len(lst), step):
            yield lst[i : i + step]

    if len(all_rows) < 1 and len(flagged_projects) < 1:
        return

    # Construct the slack message in multiple posts (due to size)
    n_chunks = ceil(
        num_chars(list(sum(all_rows, ()))) / SLACK_MESSAGE_MAX_CHARS,
    )
    logging.info(f'Breaking body into {n_chunks}')
    logging.info(f'Total num rows: {len(all_rows)}')

    # Make first chunk the flagged projects, then chunk by size after
    chunks = [flagged_projects]
    if is_monday:
        chunks = [flagged_projects, *chunk_list(all_rows, n_chunks)]

    posted_flagged = len(flagged_projects) < 1
    posted_header = False

    for chunk in chunks:
        # Only post dashboard message on first chunk
        # and switch to 'Projects' not 'Flagged Projects' after first chunk

        # Just send the chunk of text if not needing a header
        slack_message = chunk

        if not posted_flagged:
            summary_header = flagged_projects_header
            dashboard_message = flagged_projects_header_message
            slack_message = [summary_header, *chunk]
            posted_flagged = True
        elif posted_flagged and not posted_header:
            summary_header = normal_header
            dashboard_message = ' '
            slack_message = [summary_header, *chunk]
            posted_header = True

        post_single_slack_chunk(slack_message, dashboard_message)


def post_single_slack_chunk(
    slack_message: list[tuple[str, str]],
    header_message: str,
):
    """Post a single chunk of the slack message"""

    # Helper functions
    def wrap_in_mrkdwn(a: str) -> dict:
        return {'type': 'mrkdwn', 'text': a}

    n_chars = num_chars([''.join(list(a)) for a in slack_message])
    logging.info(f'Chunk rows: {len(slack_message)}')
    logging.info(f'Chunk size: {n_chars}')

    # Add header at the start
    logging.info(f'Chunk: {slack_message}')

    body = [
        wrap_in_mrkdwn('\n'.join(a[0] for a in slack_message)),
        wrap_in_mrkdwn('\n'.join(a[1] for a in slack_message)),
    ]

    mkdown_header_message = wrap_in_mrkdwn(header_message)
    blocks = [
        {'type': 'section', 'text': mkdown_header_message, 'fields': body},
    ]
    post_slack_chunk(blocks=blocks)


def get_percent_used_from_budget(
    b: budget.ListBudgetsResponse | None,
    last_month_total: float,
    currency: str,
) -> tuple[float | None, str]:
    """Get percent_used as a string from GCP billing budget"""
    percent_used = None
    percent_used_str = ''

    if not b:
        return None, ''

    inner_amount = b.amount.specified_amount
    if not inner_amount:
        return None, ''
    budget_currency = inner_amount.currency_code

    # 'units' is an int64, which is represented as a string in JSON,
    # this can be safely stored in Python3: https://stackoverflow.com/a/46699498
    budget_total = try_cast_int(inner_amount.units)
    monthly_used_float = try_cast_float(last_month_total)

    if budget_total and monthly_used_float:
        percent_used = monthly_used_float / budget_total
        percent_used_str = f'{round(percent_used * 100)}%'
        if budget_currency != currency:
            # there's a currency mismatch
            percent_used_str += (
                f' (mismatch currency, budget: {budget_currency} | total: {currency})'
            )

    else:
        logging.warning(
            'Couldn\'t determine the budget amount from the budget, '
            f'inner_amount.units: {inner_amount.units}, '
            f'monthly_used_float: {monthly_used_float}',
        )

    return percent_used, percent_used_str


def post_slack_chunk(blocks: list[dict], thread_ts: str | None = None):
    """Posts the given text as message to Slack."""
    try:
        if thread_ts:
            logging.info(f'Posting in thread {thread_ts}')

        result = slack_client.api_call(  # pylint: disable=duplicate-code
            'chat.postMessage',
            json={
                'channel': SLACK_CHANNEL,
                'blocks': json.dumps(blocks),
                'thread_ts': thread_ts,
                'reply_broadcast': False,
            },
        )
    except SlackApiError as err:
        logging.error(f'Error posting to Slack: {err}')

    logging.info(f'Slack API response: {result}')
    return result.get('ts')


if __name__ == '__main__':
    print(slack_bot_cost_report(None))
