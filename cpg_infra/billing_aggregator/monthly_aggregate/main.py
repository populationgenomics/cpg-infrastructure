# pylint: disable=import-error,no-name-in-module,unused-argument
"""A Cloud Function to update the status of genomic samples."""

import json
import asyncio
import logging
import os
from functools import cache

from datetime import datetime

from google.oauth2 import service_account
import google.cloud.bigquery as bq
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from flask import abort, Response
from pandas import DataFrame


OUTPUT_GOOGLE_SHEET = os.getenv('OUTPUT_BILLING_SHEET')
GCP_MONTHLY_BILLING_BQ_TABLE = os.getenv('BQ_MONTHLY_SUMMARY_TABLE')

assert OUTPUT_GOOGLE_SHEET, GCP_MONTHLY_BILLING_BQ_TABLE

logger = logging.getLogger('monthly-upload')


@cache
def get_bigquery_client() -> bq.Client:
    """Get instantiated cached bq client"""
    return bq.Client()


def from_request(*args, **kwargs):
    """Entrypoint for cloud functions, run always as default (previous month)"""
    asyncio.new_event_loop().run_until_complete(
        process_and_upload_monthly_billing_report(None)
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
    append_values_to_google_sheet(OUTPUT_GOOGLE_SHEET, values)


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
    creds = service_account.Credentials.from_service_account_file(
        os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    )
    scoped_creds = creds.with_scopes(scopes)

    # pylint: disable=maybe-no-member
    try:
        service = build('sheets', 'v4', credentials=scoped_creds)
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
        print(f"{(result.get('updates').get('updatedCells'))} cells appended.")
        return result

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

    test_invoice_month = None
    event_loop.run_until_complete(
        process_and_upload_monthly_billing_report(test_invoice_month)
    )
