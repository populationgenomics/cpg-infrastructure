# pylint: disable=import-error,no-name-in-module,unused-argument
"""A Cloud Function to update the status of genomic samples."""

import json
import asyncio
import logging
import os
from functools import cache

from datetime import datetime

import functions_framework
import google.auth
import google.cloud.bigquery as bq
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from flask import abort, Response, Request
from pandas import DataFrame


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
) -> tuple[datetime | None, datetime | None]:
    """
    Get the invoice month from the cloud function request.
    """
    if not request:
        return None, None

    content_type = request.content_type
    if request.method == 'GET':
        request_data = request.args
    elif content_type == 'application/json':
        request_data = request.get_json(silent=True)
    elif content_type in ('application/octet-stream', 'text/plain'):
        request_data = json.loads(request.data)
    elif content_type == 'application/x-www-form-urlencoded':
        request_data = request.form
    else:
        raise ValueError(f'Unknown content type: {content_type}')

    if request_data and 'invoice_month' in request_data:
        invoice_month = request_data.get('invoice_month')
    else:
        raise ValueError("JSON is invalid, or missing a 'invoice_month'")

    return invoice_month


@functions_framework.http
def from_request(request: Request):
    """Entrypoint for cloud functions, run always as default (previous month)"""

    try:
        invoice_month = get_invoice_month_from_request(request)
    except ValueError:
        invoice_month = None

    return asyncio.new_event_loop().run_until_complete(
        process_and_upload_monthly_billing_report(invoice_month)
    )


async def load_for_year(year):
    """Load all months for the specified year (except the current month)"""
    for month in range(1, 13):
        invoice_month = f'{year}{month}'
        if year == datetime.now().year and month == datetime.now().month:
            logger.warning(f'Skipping {invoice_month} as it is in progress')
            continue

        await process_and_upload_monthly_billing_report(invoice_month)


async def process_and_upload_monthly_billing_report(invoice_month: str = None):
    """Main entry point for the Cloud Function."""

    if not invoice_month:
        year = datetime.now().year
        month = datetime.now().month - 1
        invoice_month = f'{year}{month:0>2}'

    logging.info(f'Processing request for invoice month: {invoice_month}')

    data = get_billing_data(invoice_month)
    if len(data) == 0:
        logger.info(f'Skipping {invoice_month} with no data')
        return

    data['cost'].fillna(0)
    data['key'] = data.topic + '-' + data.month + '-' + data.cost_category
    values: list = data.values.tolist()
    updated = append_values_to_google_sheet(OUTPUT_GOOGLE_SHEET, values)

    return f'{updated} cells appended for invoice month {invoice_month}', 200


def abort_message(status: int, message: str):
    """Custom abort wrapper that allows for error messages to be passed through"""
    return abort(Response(json.dumps({'message': message}), status))


def append_values_to_google_sheet(spreadsheet_id, _values):
    """
    Creates the batch_update the user has access to.
    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """

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
                range=f'{datetime.now().year}-data',
                valueInputOption='RAW',
                body=body,
            )
            .execute()
        )
        updated = result.get('updates').get('updatedCells')
        print(f"{updated} cells appended.")
        return updated

    except HttpError as error:
        print(f'An error occurred: {error}')
        return error


def get_billing_data(invoice_month: str) -> DataFrame:
    """
    Retrieve the billing data for a particular billing month from the aggregation table
    Return results as a dataframe
    """

    _query = f"""
        SELECT * FROM `{GCP_MONTHLY_BILLING_BQ_TABLE}`
        WHERE month = @yearmonth
        ORDER BY topic
    """
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter('yearmonth', 'STRING', str(invoice_month)),
        ]
    )

    migrate_rows = (
        get_bigquery_client()
        .query(_query, job_config=job_config)
        .result()
        .to_dataframe()
    )

    return migrate_rows


if __name__ == '__main__':
    # Set logging levels
    logger.setLevel(logging.INFO)
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    event_loop = asyncio.new_event_loop()

    test_invoice_month = '202301'
    event_loop.run_until_complete(
        process_and_upload_monthly_billing_report(test_invoice_month)
    )
