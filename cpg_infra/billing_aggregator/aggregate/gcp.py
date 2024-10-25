# flake8: noqa: PGH003,ANN001,DTZ001,ARG001,ANN002,
"""
Cloud function that runs once a month that synchronises a portion of data from:

FROM:   billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343
TO:     billing-admin-290403.billing_aggregate.aggregate

Tasks:

- Needs to convert {billing_project_id} into DATSET
- Only want to transfer data from the projects in the server-config
- Can't duplicate rows, so:
    - just grab only settled data within START + END of previous time period
- Service ID should be faithfully handed over
- Should search and update for [START_PERIOD, END_PERIOD)

IMPORTANT:
    When loading gcp data it's important to know that the id generated for each
    data row...
    DOES NOT uniquely define a single row in the aggregate bq table

    Specifically, the same row validly can appear twice in the gcp billing
    data and that is reflected correctly in the aggregate table.

"""

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime
from typing import Dict, Optional

import functions_framework
import google.cloud.bigquery as bq
import rapidjson
from flask import Request

from cpg_utils.cloud import read_secret

try:
    from . import utils
except ImportError:
    import utils  # type: ignore

logger = utils.logger.getChild('gcp')
logger.setLevel(logging.INFO)


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


def from_pubsub(data, *args):
    """
    From pubsub message, get start and end time if present
    """
    start, end = utils.get_start_and_end_from_data(data)
    return asyncio.new_event_loop().run_until_complete(main(start, end))


#################
#    MIGRATE    #
#################


async def migrate_billing_data(start, end, dataset_to_topic) -> int:
    """
    Gets the billing date in the time period
    Filter out any rows that aren't in the allowed project ids
    :return: The number of migrated rows
    """

    logger.info(f'Migrating data from {start} to {end}')

    def get_topic(row: dict) -> str | None:
        return utils.billing_row_to_topic(row, dataset_to_topic)

    # to_df_iterable pages the response so it's more manageable,
    # this should reduce the need for the date-range iterator
    result = 0
    for chunk in get_billing_data(start, end).to_dataframe_iterable():
        # Add id and topic to the row
        if len(chunk) == 0:
            continue

        s = time.time()
        chunk.insert(0, 'id', chunk.apply(billing_row_to_key, axis=1))
        chunk.insert(0, 'topic', chunk.apply(get_topic, axis=1))

        # reformat labels and system labels
        chunk['labels'] = chunk['labels'].apply(
            lambda x: rapidjson.dumps(
                utils.reformat_bigqquery_labels(x),
                sort_keys=True,
            ),
        )
        chunk['system_labels'] = chunk['system_labels'].apply(
            lambda x: rapidjson.dumps(
                utils.reformat_bigqquery_labels(x),
                sort_keys=True,
            ),
        )

        mins = min(chunk.get('export_time'))
        maxf = max(chunk.get('export_time'))
        logger.info(
            f'Processed {len(chunk)} in chunk ({time.time() - s:4f}s) [{mins}, {maxf}]',
        )
        result += utils.upsert_aggregated_dataframe_into_bigquery(
            dataframe=chunk,
            window_start=start,
            window_end=end,
        )

    return result


#################
#    HELPERS    #
#################


def get_billing_data(start: datetime, end: datetime):
    """
    Retrieve the billing data from start to end date inclusive
    Return results as a dataframe
    """

    # BQ table GCP_BILLING_BQ_TABLE is partition
    # by the time records have been exported (_PARTITIONTIME)
    # We need to limit dataset by _PARTITIONTIME to reduce cost
    # We need to extend the range to ensure we get all the data
    # 60 days is a safe buffer and cut the cost significantly comparing to full scan
    _query = f"""
        SELECT
            service, sku, usage_start_time, usage_end_time, project,
            labels, system_labels, location, export_time, cost,
            currency, currency_conversion_rate, usage, credits,
            invoice, cost_type, adjustment_info
        FROM `{utils.GCP_BILLING_BQ_TABLE}`
        WHERE DATE_TRUNC(usage_end_time, DAY) BETWEEN @start AND @end
        -- The following is to limit full scan to only aprox time period +/- 60 days
        AND DATE_TRUNC(_PARTITIONTIME, DAY) BETWEEN
            TIMESTAMP(DATETIME_ADD(@start, INTERVAL -@days_filter DAY)) AND
            TIMESTAMP(DATETIME_ADD(@end, INTERVAL @days_filter DAY))
        AND project.id NOT IN UNNEST(@exclude)
    """
    exclude_projects = [utils.SEQR_PROJECT_ID, utils.ES_INDEX_PROJECT_ID]
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter('start', 'STRING', start.strftime('%Y-%m-%d')),
            bq.ScalarQueryParameter('end', 'STRING', end.strftime('%Y-%m-%d')),
            bq.ArrayQueryParameter('exclude', 'STRING', exclude_projects),
            bq.ScalarQueryParameter(
                'days_filter',
                'INT64',
                utils.BQ_LARGE_PERIOD_FILTER,
            ),
        ],
    )

    return utils.get_bigquery_client().query(_query, job_config=job_config).result()


DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'


def billing_row_to_key(row) -> str:
    """Convert a billing row to a hash which will be the row key"""
    identifier = hashlib.md5()  # noqa: S324
    d = row.to_dict()
    d['usage_end_time'] = d['usage_end_time'].strftime(DATE_FORMAT)
    d['export_time'] = d['export_time'].strftime(DATE_FORMAT)
    d['usage_start_time'] = d['usage_start_time'].strftime(DATE_FORMAT)

    identifier.update(rapidjson.dumps(d, sort_keys=True).encode())
    return identifier.hexdigest()


def get_dataset_to_topic_map() -> Dict[str, str]:
    """Get the server-config from the secret manager"""
    server_config = json.loads(
        read_secret(
            utils.ANALYSIS_RUNNER_PROJECT_ID,
            'server-config',
            fail_gracefully=False,
        ),
    )
    return {v['gcp']['projectId']: k for k, v in server_config.items()}


##############
#    MAIN    #
##############


async def main(
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> dict:
    """Main body function"""
    s, e = utils.process_default_start_and_end(start, end)
    logger.info(f'Running GCP Billing Aggregation for [{start}, {end}]')

    # Storing topic map means we don't repeatedly call to access the topic
    # data mapping for each batch
    dataset_to_topic_map = get_dataset_to_topic_map()

    # Migrate the data in batches
    # This is because depending on the start-end interval all of the billing
    # data may not be able to be held in memory during the migration
    # Memory is particularly limited for cloud functions

    result = await migrate_billing_data(s, e, dataset_to_topic_map)

    logger.info(f'Migrated a total of {result} rows')

    return {'entriesInserted': result}


if __name__ == '__main__':
    # Set logging levels

    test_start, test_end = datetime(2022, 12, 1), datetime(2023, 2, 17)
    asyncio.new_event_loop().run_until_complete(main(start=test_start, end=test_end))
