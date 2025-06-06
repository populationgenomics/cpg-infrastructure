# flake8: noqa: PGH003,ANN001,DTZ001,ARG001,ANN002,S105,PD901,DTZ007
"""
Cloud function that runs once a day that downloads data billing from ICE

Tasks performed:
1. Connect to ICA API to get JWT token
2. Download CSV Billing data for selected dates
3. Upsert data into ICA_RAW_TABLE
4. Convert / migrate data into GCP specific billing format
5. Upsert into GCP_AGGREGATE_DEST_TABLE

TODO:
- Figure out how we distribute the costs to other topics
- What to do with conversion rate (iCredit to AUD), how often we update, where we store the used conversions?
- How to link the ICA to AR GUID

"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from io import StringIO
from typing import (
    Any,
    Literal,
    Optional,
)

import aiohttp
import functions_framework
import google.cloud.bigquery as bq
import numpy as np
import pandas as pd
from flask import Request

from cpg_utils.cloud import read_secret

try:
    from . import utils
except ImportError:
    import utils  # type: ignore

logger = utils.logger.getChild('ica')
logger.setLevel(logging.INFO)


ICA_TOKEN_URL = 'https://ica.illumina.com/ica/rest/api/tokens'
ICA_API_URL = 'https://use1.platform.illumina.com/v1/usage/download'
DOMAIN = 'populationgenomics'

# TODO this conversion will be semi static, we should be assing conversion rate at the date we top up iCredits
I_CREDITS_TO_AUD = 1.54


##########################
#    INPUT PROCESSORS    #
##########################


@functions_framework.http
def from_request(request: Request):
    """
    From request object, get start and end time if present
    """
    try:
        start, end = utils.get_start_and_end_from_request(request)
    except ValueError as err:
        logger.warning(err)
        logger.warning('Defaulting to None')
        start, end = None, None

    return asyncio.new_event_loop().run_until_complete(main(start, end))


def from_pubsub(data=None, _=None):
    """
    From pubsub message, get start and end time if present
    """
    start, end = utils.get_start_and_end_from_data(data)
    return asyncio.new_event_loop().run_until_complete(main(start, end))


#################
#    MIGRATE    #
#################


def get_ica_raw_data(start: datetime, end: datetime):
    """
    Retrieve the billing data from start to end date inclusive
    Return results as a dataframe
    """

    _query = f"""
        SELECT
            id, usage_timestamp, category, sku, product, sub_tenant_name, cost, metadata,
            SAFE_CAST(100*extract(year from usage_timestamp) + extract(month from usage_timestamp) AS STRING) as month
        FROM `{utils.ICA_RAW_TABLE}`
        WHERE DATE_TRUNC(usage_timestamp, DAY) BETWEEN @start AND @end
    """
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter('start', 'STRING', start.strftime('%Y-%m-%d')),
            bq.ScalarQueryParameter('end', 'STRING', end.strftime('%Y-%m-%d')),
        ],
    )

    return utils.get_bigquery_client().query(_query, job_config=job_config).result()


async def migrate_billing_data(start, end) -> int:
    """
    Gets the billing date in the time period
    Filter out any rows that aren't in the allowed project ids
    :return: The number of migrated rows
    """
    logger.info(f'Migrating data from {start} to {end}')

    existing_ids = utils.retrieve_stored_ids(
        start - timedelta(days=1), end + timedelta(days=1), 'ica'
    )

    result = 0
    for chunk in get_ica_raw_data(start, end).to_dataframe_iterable():
        # Add new fields to the row
        if len(chunk) == 0:
            continue

        # add required GCP billing fields that are not in ICA data
        chunk.insert(0, 'topic', chunk.apply(get_topic, axis=1))
        chunk.insert(0, 'service', chunk.apply(get_service, axis=1))
        chunk.insert(0, 'labels', chunk.apply(get_labels, axis=1))
        chunk.insert(0, 'invoice', chunk.apply(get_invoice, axis=1))

        chunk['sku'] = chunk.apply(get_sku, axis=1)

        chunk['usage_start_time'] = chunk['usage_timestamp']
        chunk['usage_end_time'] = chunk['usage_timestamp']
        chunk['export_time'] = chunk['usage_timestamp']

        chunk['cost'] = I_CREDITS_TO_AUD * chunk['cost']
        chunk['currency'] = 'AUD'
        chunk['cost_type'] = 'regular'

        # keep only selected columns
        selected_chunk = chunk[
            [
                'id',
                'topic',
                'service',
                'labels',
                'sku',
                'usage_start_time',
                'usage_end_time',
                'export_time',
                'currency',
                'cost',
                'invoice',
                'cost_type',
            ]
        ].replace(
            'None', np.nan
        )  # replace all None with np.NaN os it gets converted to null

        # convert to list of dictionaries and insert into BigQuery
        result += utils.upsert_rows_into_bigquery(
            objs=json.loads(
                # Convert DataFrame to JSON string with ISO date format
                selected_chunk.to_json(orient='records', date_format='iso')
            ),
            existing_ids=existing_ids,
            dry_run=False,
        )

    return result


#################
#    HELPERS    #
#################


def get_topic(row: dict) -> str:
    return 'ica-illumina'


def get_service(row: dict) -> dict[str, Any]:
    return {
        'id': row.get('sku'),
        'description': (
            'Cloud Storage' if row.get('category') == 'Storage' else 'Compute Engine'
        ),
    }


def get_sku(row: dict) -> dict[str, Any]:
    return {'id': row.get('sku'), 'description': row.get('product')}


def get_project(row: dict) -> dict[str, Any]:
    return {'id': row.get('sub_tenant_name'), 'name': row.get('sub_tenant_name')}


def get_invoice(row: dict) -> dict[str, Any]:
    return {'month': row.get('month')}


def get_labels(row: dict) -> str | None:
    # split metadata by | and then convert to json pairs
    metadata = row.get('metadata', '')
    if not metadata:
        return None

    pairs = metadata.split('|')
    labels = {}
    for pair in pairs:
        if ':' in pair:
            key, value = pair.split(
                ':', 1
            )  # Split only once to handle values with colons
            labels[key.strip()] = value.strip()  # Remove spaces
    return json.dumps(labels)


def get_api_key() -> str | None:
    """
    Retrieve the API key for the ICE billing API from GCP Secret Manager
    Returns:
        str: The API key if it exists, otherwise None
    Raises:
        ValueError: If the secret is not found or cannot be parsed
    """
    api_key_secret = read_secret(
        'cpg-common',
        utils.ICA_API_SECRET_NAME,
        fail_gracefully=False,
    )

    if not api_key_secret:
        return None

    api_key_secret = json.loads(api_key_secret)
    return api_key_secret.get('apiKey', None)


async def get_jwt_token(
    xkey: str | None,
    domain: str,
    attempts: int = 1,
) -> str | None:
    """
    Get JWT token from ICA
    """
    if not xkey:
        raise ValueError('No api key provided for ICE API')
    try:
        resp: dict[
            Literal['token',],
            Any,
        ] = await utils.async_retry_transient_post_request(
            ICA_TOKEN_URL,
            aiohttp.ClientError,
            headers={
                'accept': 'application/vnd.illumina.v3+json',
                'X-API-Key': xkey,
            },
            data={'tenant': domain},
            attempts=attempts,
        )
        return resp.get('token')

    except asyncio.TimeoutError as ex:
        e = ex

    raise e


async def get_csv_data(
    token: str | None,
    start: datetime,
    end: datetime,
    attempts: int = 1,
) -> list[dict[str, Any]]:
    """
    Get CSV billing data from ICA
    """
    if not token:
        raise ValueError('No token provided for ICE API')
    try:
        url = (
            ICA_API_URL
            + f'?StartDate={start.strftime("%Y-%m-%d")}&EndDate={end.strftime("%Y-%m-%d")}'
        )
        resp: str | None = await utils.async_retry_transient_get_request(
            url,
            aiohttp.ClientError,
            headers={
                'accept': 'text/csv',
                'Authorization': f'Bearer {token}',
            },
            attempts=attempts,
            as_json=False,
        )
        data = StringIO(resp)
        df = pd.read_csv(data)

        # all headers in lower case
        df = df.rename(columns=str.lower)
        df['usage_timestamp'] = pd.to_datetime(
            df['usage_timestamp'], format='%m/%d/%Y %H:%M:%S'
        )
        # add unique id
        df['id'] = 'ica-' + df['usage_id'].astype(str)

        # make sku and usage_id as strings
        df['sku'] = df['sku'].astype(str)
        df['usage_id'] = df['usage_id'].astype(str)
        # export df to JSON string and reload as JSON
        return json.loads(df.to_json(orient='records', date_format='iso'))

    except asyncio.TimeoutError as ex:
        e = ex

    raise e


##############
#    MAIN    #
##############


async def main(
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> dict:
    """Main body function"""
    s, e = utils.process_default_start_and_end(start, end, timedelta(days=7))

    # ICA API only accept dates, no time
    # lets keep only date in s and e
    s = datetime.strptime(s.strftime('%Y-%m-%d'), '%Y-%m-%d')
    e = datetime.strptime(e.strftime('%Y-%m-%d'), '%Y-%m-%d')

    logger.info(f'Running ICE Billing for [{s}, {e}]')

    # Download csv file from ICE billing API
    api_key = get_api_key()
    token = await get_jwt_token(api_key, DOMAIN)
    entries = await get_csv_data(token, s, e)

    # get existing records, only insert new ones into ICA_RAW_TABLE
    existing_ids = utils.retrieve_stored_ids(
        s - timedelta(days=1),
        e + timedelta(days=1),
        'ica',
        table=utils.ICA_RAW_TABLE,  # type: ignore
        endtime_col_name='usage_timestamp',
    )
    result = utils.upsert_rows_into_bigquery(
        table=utils.ICA_RAW_TABLE,  # type: ignore
        objs=entries,
        existing_ids=existing_ids,
        dry_run=False,
        schema_func=utils.get_ica_schema_json,
    )
    logger.info(f'Inserted {result} rows')

    # migrate data to aggregated billing table
    result = await migrate_billing_data(s, e)
    logger.info(f'Migrated {result} rows')

    # TODO redistribute data to topics
    # most likely by 'sub_tenant_name' ? or by seq groups or by ARGUID
    return {'entriesInserted': result}


if __name__ == '__main__':
    # Set logging levels
    logger.setLevel(logging.INFO)
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    test_start, test_end = None, None
    asyncio.new_event_loop().run_until_complete(main(start=test_start, end=test_end))
