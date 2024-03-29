# flake8: noqa:DTZ005,DTZ006,DTZ007,PLR2004,C901,PLR2004
"""A Cloud Function to update the status of genomic samples."""

import asyncio
import json
import logging
import os
from base64 import b64decode
from datetime import date, datetime, timedelta
from functools import cache

import functions_framework
import google.auth
import google.cloud.bigquery as bq
from flask import Request, Response, abort
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pandas import DataFrame

INVOICE_DAY_DIFF = 3
OUTPUT_GOOGLE_SHEET = os.getenv('OUTPUT_BILLING_SHEET')
GCP_MONTHLY_BILLING_BQ_TABLE = os.getenv('BQ_MONTHLY_SUMMARY_TABLE')

assert OUTPUT_GOOGLE_SHEET, GCP_MONTHLY_BILLING_BQ_TABLE

logger = logging.getLogger('monthly-upload')


@cache
def get_bigquery_client() -> bq.Client:
    """Get instantiated cached bq client"""
    return bq.Client()


def get_invoice_month_from_request(
    request: Request,
) -> str | None:
    """
    Get the invoice month from the cloud function request.
    """
    if not request:
        logger.info('No request found')
        return None

    content_type = request.content_type
    if request.method == 'GET':
        logger.info(f'GET request, using args: {request.args}')
        request_data = request.args
    elif content_type == 'application/json':
        logger.info('JSON found in request')
        request_data = request.get_json(silent=True)
    elif content_type in ('application/octet-stream', 'text/plain'):
        logger.info('Text data found')
        request_data = json.loads(request.data)
    elif content_type == 'application/x-www-form-urlencoded':
        logger.info('Encoded Form')
        request_data = request.form
    else:
        logger.warning(f'Unknown content type: {content_type}. Defaulting to None.')
        raise ValueError(f'Unknown content type: {content_type}')

    if not request_data:
        logger.warning(f'Attributes could not be found in request: {request_data}')
        return None

    if message := request_data.get('message'):
        if attributes := message.get('attributes'):
            if 'invoice_month' in attributes:
                request_data = attributes
        elif 'data' in message:
            try:
                request_data = json.loads(b64decode(message['data']))
            except Exception as exp:
                raise exp

    logger.info(request_data)

    if not request_data or 'invoice_month' not in request_data:
        logger.warning('Could not find invoice_month. Default to None.')
        raise ValueError("JSON is invalid, or missing a 'invoice_month'")

    return request_data.get('invoice_month')


@functions_framework.http
def from_request(request: Request):
    """Entrypoint for cloud functions, run always as default (previous month)"""

    try:
        invoice_month = get_invoice_month_from_request(request)
    except ValueError as err:
        logger.warning(err)
        logger.warning('Defaulting to None')
        invoice_month = None

    return asyncio.new_event_loop().run_until_complete(
        process_and_upload_monthly_billing_report(invoice_month),
    )


async def load_for_year(year: int):
    """Load all months for the specified year (except the current month)"""
    for month in range(1, 13):
        invoice_month = f'{year}{month}'
        if year == datetime.now().year and month == datetime.now().month:
            logger.warning(f'Skipping {invoice_month} as it is in progress')
            continue

        await process_and_upload_monthly_billing_report(invoice_month)


async def process_and_upload_monthly_billing_report(
    invoice_month: str | None = None,
):
    """Main entry point for the Cloud Function."""

    if not invoice_month:
        year = datetime.now().year
        month = datetime.now().month - 1
        invoice_month = f'{year}{month:0>2}'

    logging.info(f'Processing request for invoice month: {invoice_month}')

    data = get_billing_data(invoice_month)
    if len(data) == 0:
        logger.info(f'Skipping {invoice_month} with no data')
        return None

    data['cost'].fillna(0)
    data['key'] = data.topic + '-' + data.month + '-' + data.cost_category
    values: list = data.to_numpy().tolist()
    updated = append_values_to_google_sheet(OUTPUT_GOOGLE_SHEET, values, invoice_month)

    return f'{updated} cells appended for invoice month {invoice_month}', 200


def abort_message(status: int, message: str):
    """Custom abort wrapper that allows for error messages to be passed through"""
    return abort(Response(json.dumps({'message': message}), status))


def append_values_to_google_sheet(
    spreadsheet_id: str,
    _values: list,
    invoice_month: str,
):
    """
    Creates the batch_update the user has access to.
    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """

    assert len(invoice_month) == 6
    year = invoice_month[:4]

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
    ]
    creds, _ = google.auth.default(scopes=scopes)

    # pylint: disable=maybe-no-member
    try:
        service = build('sheets', 'v4', credentials=creds)
        body = {'values': _values}
        result = (
            service.spreadsheets()
            .values()
            .append(
                spreadsheetId=spreadsheet_id,
                range=f'{year}-data',
                valueInputOption='RAW',
                body=body,
            )
            .execute()
        )
        updated = result.get('updates').get('updatedCells')
        logger.info(f'{updated} cells appended to sheet {spreadsheet_id}')
        return updated

    except HttpError as error:
        logger.error(f'An error occurred: {error}')
        return error


def get_invoice_month_range(convert_month: date) -> tuple[date, date]:
    """Get the start and end date of the invoice month for a given date"""
    first_day = convert_month.replace(day=1)

    # Grab the first day of invoice month then subtract INVOICE_DAY_DIFF days
    start_day = first_day + timedelta(days=-INVOICE_DAY_DIFF)

    if convert_month.month == 12:
        next_month = first_day.replace(month=1, year=convert_month.year + 1)
    else:
        next_month = first_day.replace(month=convert_month.month + 1)

    # Grab the last day of invoice month then add INVOICE_DAY_DIFF days
    last_day = next_month + timedelta(days=-1) + timedelta(days=INVOICE_DAY_DIFF)

    return start_day, last_day


def get_billing_data(invoice_month: str) -> DataFrame:
    """
    Retrieve the billing data for a particular billing month from the aggregation table
    Return results as a dataframe
    """
    assert GCP_MONTHLY_BILLING_BQ_TABLE

    if '`' in GCP_MONTHLY_BILLING_BQ_TABLE:
        raise ValueError(
            f'Do not include backticks in the table ({GCP_MONTHLY_BILLING_BQ_TABLE})',
        )

    invoice_month_date = datetime.strptime(invoice_month, '%Y%m').date()
    window_start, window_end = get_invoice_month_range(invoice_month_date)
    _query = f"""
        SELECT * FROM `{GCP_MONTHLY_BILLING_BQ_TABLE}`
        WHERE month = @invoice_month
        AND DATE_TRUNC(usage_end_time, DAY) BETWEEN @window_start AND @window_end
        ORDER BY topic
    """  # noqa: S608
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter('invoice_month', 'STRING', str(invoice_month)),
            bq.ScalarQueryParameter(
                'window_start',
                'STRING',
                window_start.strftime('%Y-%m-%d'),
            ),
            bq.ScalarQueryParameter(
                'window_end',
                'STRING',
                window_end.strftime('%Y-%m-%d'),
            ),
        ],
    )

    return (
        get_bigquery_client()
        .query(_query, job_config=job_config)
        .result()
        .to_dataframe()
    )


if __name__ == '__main__':
    # Set logging levels
    logger.setLevel(logging.INFO)
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    event_loop = asyncio.new_event_loop()

    event_loop.run_until_complete(process_and_upload_monthly_billing_report(None))
