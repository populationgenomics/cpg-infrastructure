# pylint: disable=too-many-locals
"""A Cloud Function to send a daily GCP cost report to Slack."""

import calendar
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta
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
  ) day
    ON month.id = day.id
    AND month.currency = day.currency
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


def get_days_in_this_month() -> int:
    # Assuming TIMEZONE is defined elsewhere and is a timezone-aware object
    current_date = datetime.now(tz=TIMEZONE)
    current_year, current_month = current_date.year, current_date.month

    # Get the number of days in the current month
    _, days_in_this_month = calendar.monthrange(current_year, current_month)

    return days_in_this_month


def billing_link(project_id: str) -> str:
    yesterday = datetime.now(tz=TIMEZONE).date() - timedelta(days=1)
    yesterday_string = yesterday.strftime('%Y-%m-%d')
    return (
        BILLING_URL
        + f'&selectedData={project_id}&start={yesterday_string}&end={yesterday_string}'
    )


def format_billing_row(
    fields: dict,
    currency: str,
    project_id: str | None = None,
) -> tuple[SortKey, str | None, str]:
    """
    Formats the billing row for a project.

    Args:
        project_id (str | None): The ID of the project.
        fields (dict): The billing data for the project.
        currency (str): The currency used for formatting.
        percent_threshold (float, optional): The threshold for percent used. Defaults to 0.

    Returns:
        tuple[float, str, str]: The formatted billing row.

    """

    # Helper function to format money values
    def money_format(money: float | None) -> str | None:
        if money is None:
            return None
        if money < TINY_VALUE_THRESHOLD:
            return None
        if money > HUNDREDS_ROUNDING_THRESHOLD:
            return f'${money:.0f}'
        return f'${money:.2f}'

    # Helper function to format cost categories
    def format_cost_categories(data: dict, currency: str, perc_used: float) -> str:
        currency = ' ' + currency if 'AUD' not in currency else ''

        # percentage used string
        suffix = currency + f' ({perc_used:.0%})' if perc_used else ''

        # Otherwise, sort by cost category and format the values
        values = [
            money_format(data[k])
            for k in sorted(data.keys())
            if money_format(data[k]) is not None
        ]

        if len(values) == 0:
            return None

        values = [v if v else '$0' for v in values]

        # Then join with a '+' and return
        return ' + '.join(values) + suffix

    # Get the budget for the project if available
    # Returns 0 and '' if no budget is available
    budget_map = get_budget_map()
    day_total = sum([x for x in fields['day'].values() if x])
    daily_percent_used, monthly_percent_used = get_percent_used_from_budget(
        budget=budget_map.get(project_id),
        day_total=day_total,
        month_total=fields['month'].get('total', 0),
        currency=currency,
    )

    # Format cost categories for daily and monthly costs
    url = billing_link(project_id)
    project_link: str = project_id
    if url:
        project_link = f'<{url}|{project_id}>'

    row_str_1: str = format_cost_categories(fields['day'], currency, daily_percent_used)
    row_str_2: str = format_cost_categories(
        fields['month'],
        currency,
        monthly_percent_used,
    )

    # Work out if it's a flagged project or not
    # To be flagged, the percent used must be above the threshold which is the % through
    # the month.
    # That or if the spending in the past days is over 2 days of the budget
    flagged_project = (
        monthly_percent_used is not None and monthly_percent_used >= month_progress()
    ) or (
        daily_percent_used is not None
        and daily_percent_used >= (2 / get_days_in_this_month())
    )

    # potential formatting
    if flagged_project:
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
        flagged_project,
        sum(x for x in fields['day'].values() if x),
        sum(x for x in fields['month'].values() if x),
    )

    # Placeholder string for no data
    row_str_1 = row_str_1 if row_str_1 else 'No daily cost'
    row_str_2 = row_str_2 if row_str_2 else 'No monthly cost'

    return sort_key, project_link, row_str_1 + ' | ' + row_str_2


def num_chars(lst: list[str]) -> int:
    return len(''.join(lst))


@functions_framework.http
def slack_bot_cost_report(request: flask.Request):
    """
    Main entry point for the Cloud Function.

    This function generates a cost report for Google Cloud Platform (GCP) projects.
    It retrieves cost data from BigQuery and calculates the total cost for each project,
    broken down by currency and cost category. The function then formats the cost report
    and sends it as a message to Slack.
    """

    force_run = False
    if request.method == 'POST' and 'application/json' in request.headers.get(
        'Content-Type',
    ):
        request_json = request.get_json()
        force_run = request_json.get('force_run', False)

    totals: dict[str, dict[str, dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(float)),
    )

    # Slack message header and summary
    project_summary: dict[str, dict[str, Any]] = {}
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

        totals[currency][cost_category]['month'] += row.get('month', 0)

        if row['day']:
            totals[currency][cost_category]['day'] += row.get('day', 0)

        grouped_rows[project_id][currency]['day']['total'] += row.get('day', 0) or 0
        grouped_rows[project_id][currency]['month']['total'] += row.get('month', 0)

    # Format the billing rows and add them to the project summary
    for project_id, by_currency in grouped_rows.items():
        for currency, row in by_currency.items():
            sort_key, prj_link, row_str = format_billing_row(
                row,
                currency,
                project_id,
            )
            project_summary[project_id] = {
                'sort': sort_key,
                'value': (prj_link, row_str),
            }

    if len(totals) == 0:
        logging.info(
            "No information to log, this function won't log anything to slack.",
        )
        return 'Nothing to log', 204

    # Format the totals and add them to the totals summary
    for currency, by_category in totals.items():
        fields: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        day_total: float = 0
        month_total: float = 0

        for vals in by_category.values():
            last_day = vals['day']
            last_month = vals['month']
            day_total += last_day
            month_total += last_month
            fields['day']['total'] += last_day
            fields['month']['total'] += last_month

        # totals don't have percent used
        _, _, row_str = format_billing_row(fields, currency)
        totals_summary.append(
            (f'<{BILLING_URL}|*All projects:*>', row_str),
        )

        post_slack_message(project_summary, totals_summary, force_run)

    return 'Success', 200


def post_slack_message(
    project_summary: dict[str, dict[str, Any]],
    totals_summary: list[tuple[str, str]],
    force_run: bool = False,
):
    """Post the slack message with the project summary and totals summary"""
    flagged_projects_header = [
        '*Flagged Projects*',
        '*Previous Day (%) | Month (%)*',
    ]
    normal_header = [
        '*Projects*',
        '*Previous Day (%) | Month (%)*',
    ]
    flagged_projects_header_message = (
        f'We are {month_progress():.0%} through the month. Costs exceeding '
        f'the monthly budget or daily limit ({2 * (1 / get_days_in_this_month()):.1%})'
        ' are flagged below:'
    )

    # Processing the projects summary into flagged and not flagged
    project_summary_keys_sorted = sorted(
        project_summary.keys(),
        key=lambda x: project_summary[x]['sort'],
        reverse=True,
    )
    flagged_projects = [
        project_summary[x]['value']
        for x in project_summary_keys_sorted
        if project_summary[x]['sort'][0]
    ]

    # Sort the projects by the sort key
    sorted_projects = [
        project_summary[x]['value']
        for x in project_summary_keys_sorted
        if not project_summary[x]['sort'][0]
    ]

    is_monday = datetime.now(tz=TIMEZONE).weekday() == 0

    # Next, if we are posting today add hail to the flagged projects at the bottom
    hail_project = [x for x in project_summary_keys_sorted if 'hail' in x].pop()
    flagged_projects.append(project_summary[hail_project]['value'])
    all_rows = [*totals_summary, *sorted_projects]

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

    # Only add all projects on a Monday
    if force_run or is_monday:
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
    budget: budget.ListBudgetsResponse | None = None,
    day_total: float | None = None,
    month_total: float | None = None,
    currency: str | None = None,
) -> tuple[float | None, float | None]:
    """Get percent_used as a string from GCP billing budget"""
    if not budget:
        return None, None

    inner_amount = budget.amount.specified_amount
    if not inner_amount:
        return None, None
    budget_currency = inner_amount.currency_code

    if budget_currency != currency:
        return None, None

    # 'units' is an int64, which is represented as a string in JSON,
    # this can be safely stored in Python3: https://stackoverflow.com/a/46699498
    budget_total = try_cast_int(inner_amount.units)
    daily_used_float = try_cast_float(day_total)
    monthly_used_float = try_cast_float(month_total)

    if None in (budget_total, daily_used_float, monthly_used_float):
        logging.warning(
            "Couldn't determine the budget amount from the budget, "
            f'inner_amount.units: {inner_amount.units}, '
            f'daily_used_float: {daily_used_float}, '
            f'monthly_used_float: {monthly_used_float}',
        )
        return None, None

    # Inputs valid and converted to float
    # Now calculate the percent used daily and monthly
    day_percent_used = daily_used_float / budget_total
    month_percent_used = monthly_used_float / budget_total

    return day_percent_used, month_percent_used


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
