# flake8: noqa: ARG001,ANN001,ANN002,ANN401,ERA001,PGH003,DTZ001,C901
"""
A cloud function that synchronises HAIL billing data to BigQuery

TO:     billing-admin-290403.billing_aggregate.aggregate

Notes:

- The service.id should be 'hail'

Tasks:

- Only want to transfer data from the projects in the server-config
- Need to build an endpoint in Hail to service this metadata
    - Take in a project and a date range in the endpoint
- Transform into new generic format
    - One entry per resource type per batch

- Can't duplicate rows, so determine some ID:
    - Only sync 'settled' jobs within datetimes
        (ie: finished between START + END of previous time period)
"""

import argparse
import asyncio
import base64
import gzip
import json
import logging
import os
import shutil
from datetime import datetime, timezone
from typing import Any, Generator

import functions_framework
from flask import Request

from cpg_utils.cloud import read_secret
from cpg_utils.config import AR_GUID_NAME

try:
    from . import utils
except ImportError:
    import utils  # type: ignore

SERVICE_ID = 'hail'
EXCLUDED_BATCH_PROJECTS = {'hail', 'seqr'}


logger = utils.logger
logger = logger.getChild('hail')
logger.propagate = False


def get_billing_projects():
    """
    Get Hail billing projects, same names as dataset names
    """
    server_config = read_secret(
        utils.ANALYSIS_RUNNER_PROJECT_ID,
        'server-config',
        fail_gracefully=False,
    )

    if not server_config:
        return []

    server_config = json.loads(server_config)

    return list(set(server_config.keys()) - EXCLUDED_BATCH_PROJECTS)


def get_finalised_entries_for_batch(
    batch: utils.BatchType,
    jobs: list[utils.JobType],
) -> Generator[dict[str, Any], None, None]:
    """
    Take a batch, and generate the actual cost of all the jobs,
    and return a list of BigQuery rows - one per resource type.
    """

    if batch['billing_project'] in EXCLUDED_BATCH_PROJECTS:
        return None

    start_time = utils.parse_hail_time(batch['time_created'])
    if not start_time:
        raise ValueError(f'No start time for batch {batch["id"]}')

    end_time = utils.parse_hail_time(batch['time_completed'])
    batch_id = batch['id']
    namespace = utils.infer_batch_namespace(batch)
    dataset = str(batch['billing_project'])
    if dataset.lower() == 'ci':
        # Keep CI jobs in the hail topic
        dataset = 'hail'

    currency_conversion_rate = utils.get_currency_conversion_rate_for_time(start_time)
    attributes = batch.get('attributes', {})
    batch_url = utils.HAIL_UI_URL.replace('{batch_id}', str(batch_id))
    if 'ar_guid' in attributes:
        # sneaky rename
        attributes[AR_GUID_NAME] = attributes.pop('ar_guid')

    for job in jobs:
        for batch_resource, raw_cost in job['cost'].items():
            if batch_resource.startswith('service-fee'):
                continue

            job_id = job['job_id']

            labels = {
                'dataset': dataset,
                'batch_id': str(batch_id),
                'job_id': str(job_id),
                'batch_resource': batch_resource,
                'batch_name': attributes.get('name'),
                'url': batch_url,
                'namespace': namespace,
            }

            # Add all batch attributes, removing any duped labels
            labels.update(attributes)
            labels.update(job.get('attributes', {}))
            if labels.get('name'):
                labels['job_name'] = labels.pop('name')
            if compressed_b64 := labels.pop('sequencing_groups_gzip', None):
                compressed = base64.standard_b64decode(compressed_b64)
                labels['sequencing_groups'] = gzip.decompress(compressed).decode()

            # Remove any labels with falsey values e.g. None, '', 0
            labels = dict(filter(lambda lbl: lbl[1], labels.items()))

            cost = utils.get_total_hail_cost(
                currency_conversion_rate,
                raw_cost=raw_cost,
            )
            usage = job['resources'].get(batch_resource, 0)

            # 2023-03-07 mfranklin: I know this key isn't unique, but to avoid issues
            # with changing the resource_id again, we'll only use the batch_id + job_id
            # as the key as it's sensible for us to assume that all the entries exist if
            # one of the entries exists.
            # 2023-11-23 mfranklin: Later Michael here, I've changed my mind. We need
            # to make the key unique, so we'll use the batch_id + job_id + resource_id
            # from 2023-01-01 onwards. We've migrated that data, so we're good to go.

            key_components: tuple[str, ...]
            if start_time < datetime(2023, 1, 1).astimezone(timezone.utc):
                key_components = (
                    SERVICE_ID,
                    dataset,
                    'batch',
                    str(batch_id),
                    'job',
                    str(job_id),
                )
            else:
                key_components = (
                    SERVICE_ID,
                    dataset,
                    'batch',
                    str(batch_id),
                    'job',
                    str(job_id),
                    batch_resource,
                )
            key = '-'.join(key_components).replace('/', '-')
            entry = utils.get_hail_entry(
                key=key,
                topic=dataset,
                service_id=SERVICE_ID,
                description='Hail compute',
                cost=cost,
                currency_conversion_rate=currency_conversion_rate,
                usage=usage,
                batch_resource=batch_resource,
                start_time=start_time,
                end_time=end_time,
                labels=labels,
            )
            yield entry
            yield utils.get_credit(
                entry=entry,
                topic='hail',
                project=utils.HAIL_PROJECT_FIELD,
            )


@functions_framework.http
def from_request(request: Request):
    """
    From request object, get start and end time if present
    """
    batch_ids = utils.get_batch_ids_from_request(request)
    if batch_ids:
        # batch id's were provided, so we only process those
        return asyncio.new_event_loop().run_until_complete(process_batch_ids(batch_ids))

    try:
        start, end = utils.get_start_and_end_from_request(request)
    except ValueError as err:
        logger.warning(err)
        logger.warning('Defaulting to None')
        start, end = None, None

    return asyncio.new_event_loop().run_until_complete(main(start, end))


async def process_batch_ids(batch_ids: list[str]):
    """
    Process batch ids
    """
    # locate start and end time from batch ids
    start, end = await utils.get_start_end_date_from_batches(batch_ids)
    return await main(start, end, batch_ids=batch_ids)


def from_pubsub(data=None, _=None):
    """
    From pubsub message, get start and end time if present
    """
    start, end = utils.get_start_and_end_from_data(data)
    return asyncio.new_event_loop().run_until_complete(main(start, end))


async def main(
    start: datetime | None = None,
    end: datetime | None = None,
    mode: str = 'prod',
    output_path: str | None = None,
    batch_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Main body function"""
    logger.info(f'Running Hail Billing Aggregation for [{start}, {end}]')
    start, end = utils.process_default_start_and_end(start, end)

    # result = await migrate_hail_data(start, end, hail_token, dry_run=dry_run)
    if output_path:
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        os.makedirs(output_path, exist_ok=True)
    result = await utils.process_entries_from_hail_in_chunks(
        start=start,
        end=end,
        service_id=SERVICE_ID,
        func_get_finalised_entries_for_batch=get_finalised_entries_for_batch,
        mode=mode,
        output_path=output_path,
        batch_ids=batch_ids,
    )

    logger.info(f'Migrated a total of {result} rows')

    return {'entriesInserted': result}


if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    # Check start and end date provided
    parser = argparse.ArgumentParser(description="Loading Hail billing data.")
    parser.add_argument("--start", nargs='?', help="Start date of period to load")
    parser.add_argument("--end", nargs='?', help="End date of period to load")
    args = parser.parse_args()

    if args.start and args.end:
        start_date = datetime.fromordinal(
            datetime.strptime(args.start, '%Y-%m-%d').toordinal()  # noqa: DTZ007
        )
        end_date = datetime.fromordinal(
            datetime.strptime(args.end, '%Y-%m-%d').toordinal()  # noqa: DTZ007
        )

        # iterate over the period if start_date/end_date is not none
        for period in utils.date_range_iterator(start_date, end_date):
            (start, end) = period
            asyncio.new_event_loop().run_until_complete(
                main(start=start, end=end, mode='prod')
            )
    else:
        asyncio.new_event_loop().run_until_complete(
            main(
                start=None,
                end=None,
                mode='prod',
                # output_path=os.path.join(os.getcwd(), 'hail')
            )
        )
