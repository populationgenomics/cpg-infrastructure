# pylint: disable=logging-format-interpolation,import-error
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
import asyncio
import json
import logging
import os
import shutil
from datetime import datetime
from typing import Dict, List

import functions_framework
from cpg_utils.cloud import read_secret
from cpg_utils.config import AR_GUID_NAME
from flask import Request

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

    server_config = json.loads(
        read_secret(
            utils.ANALYSIS_RUNNER_PROJECT_ID, 'server-config', fail_gracefully=False
        )
    )
    ds = list(set(server_config.keys()) - EXCLUDED_BATCH_PROJECTS)
    return ds


def get_finalised_entries_for_batch(batch: dict) -> List[Dict]:
    """
    Take a batch, and generate the actual cost of all the jobs,
    and return a list of BigQuery rows - one per resource type.
    """

    if batch['billing_project'] in EXCLUDED_BATCH_PROJECTS:
        return []

    entries = []

    start_time = utils.parse_hail_time(batch['time_created'])
    end_time = utils.parse_hail_time(batch['time_completed'])
    batch_id = batch['id']
    dataset = batch['billing_project']
    currency_conversion_rate = utils.get_currency_conversion_rate_for_time(start_time)
    attributes = batch.get('attributes', {})
    batch_url = utils.HAIL_UI_URL.replace('{batch_id}', str(batch_id))
    if 'ar_guid' in attributes:
        # sneaky rename
        attributes[AR_GUID_NAME] = attributes.pop('ar_guid')

    for job in batch['jobs']:
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
            }

            # Add all batch attributes, removing any duped labels
            labels.update(attributes)
            labels.update(job.get('attributes', {}))
            if labels.get('name'):
                labels.pop('name')

            # Remove any labels with falsey values e.g. None, '', 0
            labels = dict(filter(lambda lbl: lbl[1], labels.items()))

            cost = utils.get_total_hail_cost(
                currency_conversion_rate, raw_cost=raw_cost
            )
            usage = job['resources'].get(batch_resource, 0)

            # 2023-03-07 mfranklin: I know this key isn't unique, but to avoid issues
            # with changing the resource_id again, we'll only use the batch_id + job_id
            # as the key as it's sensible for us to assume that all the entries exist if
            # one of the entries exists.
            key = '-'.join(
                (
                    SERVICE_ID,
                    dataset,
                    'batch',
                    str(batch_id),
                    'job',
                    str(job_id),
                )
            ).replace('/', '-')
            entries.append(
                utils.get_hail_entry(
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
            )

    entries.extend(
        utils.get_credits(
            entries=entries, topic='hail', project=utils.HAIL_PROJECT_FIELD
        )
    )

    return entries


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


async def main(
    start: datetime = None,
    end: datetime = None,
    mode: str = 'prod',
    output_path: str = None,
) -> dict:
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
        func_get_finalised_entries_for_batch=get_finalised_entries_for_batch,
        mode=mode,
        output_path=output_path,
    )

    logger.info(f'Migrated a total of {result} rows')

    return {'entriesInserted': result}


if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    test_start, test_end = None, None
    asyncio.new_event_loop().run_until_complete(
        main(
            start=test_start,
            end=test_end,
            mode='prod',
            # output_path=os.path.join(os.getcwd(), 'hail'),
        )
    )
