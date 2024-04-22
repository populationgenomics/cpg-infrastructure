# flake8: noqa: PGH003,ANN001,ERA001,DTZ007,DTZ001,C901
"""
This cloud function runs DAILY, and distributes the cost of
SEQR on the sample size within SEQR.

- It first pulls the cost of the seqr project (relevant components within it):
    - Elasticsearch, instance cost
    - Cost of loading data into seqr might be difficult:
        - At the moment this in dataproc, so covered by previous result
        - Soon to move to Hail Query, which means we have to query hail
            to get the 'cost of loading'
- It determines the relative heuristic (of all projects loaded into seqr):
    - EG: 'relative size of GVCFs by project', eg:
        - $DATASET has 12GB of GVCFs of a total 86GB of all seqr GVCFs,
        - therefore it's relative heuristic is 12/86 = 0.1395,
            and share of seqr cost is 13.95%

- Insert rows in aggregate cost table for each of these costs:
    - The service.id should be 'seqr' (or something similar)
    - Maybe the label could include the relative heuristic


TO DO :

- Add cram size to SM
- Ensure getting latest joint call is split by sequence type,
    or some other metric (exome vs genome)
- Getting latest cram for sample by sequence type (eg: exome / genome)
"""
import asyncio
import copy
import dataclasses
import hashlib
import logging
import os
import shutil
from datetime import date, datetime, timezone
from typing import Any, Generator, Literal

import functions_framework
import google.cloud.bigquery as bq
import rapidjson
from flask import Request

from cpg_utils.config import AR_GUID_NAME
from metamist.apis import AnalysisApi, ProjectApi, SampleApi
from metamist.model.body_get_proportionate_map import BodyGetProportionateMap
from metamist.model.proportional_date_temporal_method import (
    ProportionalDateTemporalMethod,
)

try:
    from . import utils
except ImportError:
    import utils  # type: ignore


# ie: [(datetime, {[dataset]: (fractional_breakdown, size_of_dataset (bytes))})}
# eg: [('2022-01-06', {'d1': (0.3, 30TB), 'd2': (0.5, 50TB, 'd3': (0.2, 20TB)})]
ProportionateMapType = list[tuple[date, dict[str, tuple[float, int]]]]

SERVICE_ID = 'seqr'
SEQR_HAIL_BILLING_PROJECT = 'seqr'
ES_ANALYSIS_OBJ_INTRO_DATE = date(2022, 6, 21)

SEQR_FIRST_LOAD = date(2021, 9, 1)
SEQR_ROUND = 6

BASE = 'https://batch.hail.populationgenomics.org.au'
BATCHES_API = BASE + '/api/v1alpha/batches'
JOBS_API = BASE + '/api/v1alpha/batches/{batch_id}/jobs/resources'

JOB_ATTRIBUTES_IGNORE = {'dataset', 'samples'}
RunMode = Literal['prod', 'local', 'dry-run']

logger = utils.logger.getChild('seqr')

papi = ProjectApi()
sapi = SampleApi()
aapi = AnalysisApi()


def get_finalised_entries_for_batch(
    batch: utils.BatchType,
    jobs: list[utils.JobType],
    proportion_map: ProportionateMapType,
) -> Generator[dict[str, Any], None, None]:
    """
    Take a batch dictionary, and the full proportion map
    and return a list of entries for the Hail batch.
    """

    batch_id = batch['id']
    batch_attributes = batch.get('attributes', {})
    namespace = utils.infer_batch_namespace(batch)
    batch_name = batch_attributes.get('name')
    ar_guid = batch_attributes.get(AR_GUID_NAME, batch_attributes.get('ar_guid'))

    start_time = utils.parse_hail_time(batch['time_created'])
    end_time = utils.parse_hail_time(batch['time_completed'])

    # Assign all seqr cost to seqr topic before first ever load
    # Otherwise, determine proportion cost across topics
    if start_time.date() < SEQR_FIRST_LOAD:
        prop_map = {'seqr': (1.0, 1)}
    else:
        _, prop_map = get_ratios_from_date(start_time.date(), proportion_map)

    currency_conversion_rate = utils.get_currency_conversion_rate_for_time(start_time)

    jobs_with_no_dataset: list[utils.JobType] = []

    for job in jobs:
        dataset = job['attributes'].get('dataset', '').replace('-test', '')
        if not dataset:
            jobs_with_no_dataset.append(job)
            continue

        for entry in get_finalised_entries_for_dataset_batch_and_job(
            dataset=dataset,
            batch_id=batch_id,
            batch_name=batch_name,
            batch_start_time=start_time,
            batch_end_time=end_time,
            namespace=namespace,
            job=job,
            ar_guid=ar_guid,
            currency_conversion_rate=currency_conversion_rate,
        ):
            yield entry
            yield utils.get_credit(
                entry=entry,
                topic='hail',
                project=utils.SEQR_PROJECT_FIELD,
            )

    # Now go through each job within the batch withOUT a dataset
    # and proportion a fraction of them to each relevant dataset.
    for job in jobs_with_no_dataset:
        job_id = job['job_id']
        if not job['cost']:
            continue

        for batch_resource, raw_cost in job['cost'].items():
            if batch_resource.startswith('service-fee'):
                continue

            hail_ui_url = utils.HAIL_UI_URL.replace('{batch_id}', str(batch_id))

            labels = {
                'batch_name': batch_name,
                'batch_id': str(batch_id),
                'batch_resource': batch_resource,
                'url': hail_ui_url,
                'job_id': str(job_id),
                'namespace': namespace,
            }

            if labels.get('name'):
                labels['job_name'] = labels.pop('name')

            if ar_guid:
                labels[AR_GUID_NAME] = ar_guid

            for k, v in job.get('attributes', {}).items():
                if k in JOB_ATTRIBUTES_IGNORE:
                    continue
                if k == 'stage' and not v:
                    logger.info(f'Empty stage for {batch_id}/{job_id}')

                if k == 'ar_guid':
                    k = 'ar-guid'  # noqa: PLW2901

                labels[k] = str(v)

            # Remove any labels with falsey values e.g. None, '', 0
            labels = dict(filter(lambda lbl: lbl[1], labels.items()))

            gross_cost = utils.get_total_hail_cost(
                currency_conversion_rate,
                raw_cost=raw_cost,
            )
            raw_usage = job['resources'].get(batch_resource, 0)

            # Distribute the remaining cost across all datasets proportionally
            for dataset, (fraction, dataset_size) in prop_map.items():
                # 2023-03-07 mfranklin: I know this key isn't unique, but to avoid
                # issues with changing the resource_id again, we'll only use the
                # dataset to distribute to, batch_id, job_id as the key as it's
                # sensible for us to assume that all the entries exist
                # (for each resource) if one of the entries exists
                # 2023-11-23 mfranklin: Later Michael here, I've changed my mind. We need
                # to make the key unique, so we'll use the batch_id + job_id + resource_id
                # from 2023-01-01 onwards. We've migrated that data, so we're good to go.

                key_components: tuple[str, ...]
                if start_time < datetime(2023, 1, 1).astimezone(timezone.utc):
                    key_components = (
                        SERVICE_ID,
                        'distributed',
                        dataset,
                        'batch',
                        str(batch_id),
                        'job',
                        str(job_id),
                    )
                else:
                    key_components = (
                        SERVICE_ID,
                        'distributed',
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
                    description='Seqr compute (distributed)',
                    cost=gross_cost * fraction,
                    currency_conversion_rate=currency_conversion_rate,
                    usage=round(raw_usage * fraction),
                    batch_resource=batch_resource,
                    start_time=start_time,
                    end_time=end_time,
                    labels={
                        **labels,
                        'dataset': dataset,
                        # awkward way to round to 2 decimal places in Python
                        'fraction': str(round(100 * fraction) / 100),
                        'dataset_size': str(dataset_size),
                    },
                )
                yield entry
                yield utils.get_credit(
                    entry=entry,
                    topic='hail',
                    project=utils.SEQR_PROJECT_FIELD,
                )


def get_finalised_entries_for_dataset_batch_and_job(
    dataset: str,
    batch_id: int,
    batch_name: str,
    batch_start_time: datetime,
    batch_end_time: datetime,
    namespace: str | None,
    job: utils.JobType,
    currency_conversion_rate: float,
    ar_guid: str | None,
) -> Generator[dict[str, Any], None, None]:
    """
    Get the list of entries for a dataset attributed job

    """

    job_id = job['job_id']
    job_name = job['attributes'].get('name')

    hail_ui_url = utils.HAIL_UI_URL.replace('{batch_id}', str(batch_id))

    labels = {
        'dataset': dataset,
        'job_name': job_name,
        'batch_name': batch_name,
        'batch_id': str(batch_id),
        'job_id': str(job_id),
        'namespace': namespace,
    }

    if ar_guid:
        labels[AR_GUID_NAME] = ar_guid

    for k, v in job.get('attributes', {}).items():
        if k in JOB_ATTRIBUTES_IGNORE:
            continue
        if k == 'stage' and not v:
            logger.info('Empty stage')
        labels[k] = str(v)

    # Remove any labels with falsey values e.g. None, '', 0
    labels = dict(filter(lambda lbl: lbl[1], labels.items()))

    for batch_resource, raw_cost in job['cost'].items():
        if batch_resource.startswith('service-fee'):
            continue

        labels['batch_resource'] = batch_resource
        labels['url'] = hail_ui_url

        cost = utils.get_total_hail_cost(currency_conversion_rate, raw_cost=raw_cost)

        # 2023-03-07 mfranklin: I know this key isn't unique, but to avoid issues
        # with changing the resource_id again, we'll only use the dataset to distribute,
        # batch_id, job_id as the key as it's sensible for us to assume that all the
        # entries exist (for each resource) if one of the entries exists
        key = '-'.join(
            (
                SERVICE_ID,
                dataset,
                'batch',
                str(batch_id),
                'job',
                str(job_id),
            ),
        )
        yield utils.get_hail_entry(
            key=key,
            topic=dataset,
            service_id=SERVICE_ID,
            description='Seqr compute',
            cost=cost,
            currency_conversion_rate=currency_conversion_rate,
            usage=job['resources'].get(batch_resource, 0),
            batch_resource=batch_resource,
            start_time=batch_start_time,
            end_time=batch_end_time,
            labels=labels,
        )


def migrate_entries_from_bq(
    start: datetime,
    end: datetime,
    prop_map: ProportionateMapType,
    mode: RunMode,
    output_path: str | None,
) -> int:
    """
    Migrate entries from BQ to GCP, using the given proportionate maps
    """

    logger.debug('Migrating seqr data to BQ')
    result = 0
    istart, iend = utils.process_default_start_and_end(start, end)
    logger.info(f'Migrating seqr BQ data [{istart.isoformat()}, {iend.isoformat()}]')
    # pylint: disable=too-many-branches
    _query = f"""
        SELECT
            service, sku, usage_start_time, usage_end_time, labels, system_labels,
            location, export_time, cost, currency, currency_conversion_rate, usage,
            credits, invoice, cost_type, adjustment_info
        FROM `{utils.GCP_BILLING_BQ_TABLE}`
        WHERE DATE_TRUNC(usage_end_time, DAY) BETWEEN @start AND @end
            AND project.id IN UNNEST(@projects)
        ORDER BY usage_start_time
    """

    projects = [utils.SEQR_PROJECT_ID, utils.ES_INDEX_PROJECT_ID]
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter('start', 'STRING', istart.strftime('%Y-%m-%d')),
            bq.ScalarQueryParameter('end', 'STRING', iend.strftime('%Y-%m-%d')),
            bq.ArrayQueryParameter('projects', 'STRING', projects),
        ],
    )

    existing_ids = utils.retrieve_stored_ids(
        start,
        end,
        SERVICE_ID,
        table=utils.GCP_AGGREGATE_DEST_TABLE,
    )

    temp_file = f'seqr-query-{istart.isoformat()}-{iend.isoformat()}.json'

    json_objs_iter: Generator[dict, None, None] | list[dict]

    if mode == 'local' and os.path.exists(temp_file):
        logger.info(f'Loading BQ data from {temp_file}')
        with open(temp_file, encoding='utf-8') as f:
            json_objs_iter = [rapidjson.load(f)]
    else:
        logger.info('Querying BQ for seqr data')
        df_bq_result = (
            utils.get_bigquery_client().query(_query, job_config=job_config).result()
        )
        if mode == 'local':
            # page everything to disk, slower
            json_str = df_bq_result.to_dataframe().to_json(orient='records')
            with open(temp_file, 'w+', encoding='utf-8') as f:
                f.write(json_str)
            json_objs_iter = [rapidjson.loads(json_str)]
        else:

            def generator() -> Generator[dict, None, None]:
                for df in df_bq_result.to_dataframe_iterable():
                    logger.info('Received another page of records from bigquery')
                    yield df.to_dict(orient='records')

            json_objs_iter = generator()

    def _append_if_not_present(entries: list[dict], obj: dict) -> None:
        """
        Append records if not already in the table
        """
        if obj['id'] not in existing_ids:
            # only insert if it's not already in the table
            entries.append(obj)

    for json_objs in json_objs_iter:
        entries: list[dict] = []
        seqr_wide_param_map, current_date = None, None
        for obj in json_objs:
            labels = utils.reformat_bigqquery_labels(obj['labels'])

            usage_start_time = utils.get_date_time_from_value(
                'usage_start_time',
                obj['usage_start_time'],
            )
            usage_end_time = utils.get_date_time_from_value(
                'usage_end_time',
                obj['usage_end_time'],
            )
            dates = ['usage_start_time', 'usage_end_time', 'export_time']
            for k in dates:
                obj[k] = utils.to_bq_time(utils.get_date_time_from_value(k, obj[k]))

            # Assign all seqr cost to seqr topic before first ever load
            # Otherwise, determine proportion cost across topics
            if usage_start_time.date() < SEQR_FIRST_LOAD:
                seqr_wide_param_map = {'seqr': (1.0, 1)}
            elif current_date is None or usage_start_time.date() > current_date:
                current_date, seqr_wide_param_map = get_ratios_from_date(
                    dt=usage_end_time.date(),
                    prop_map=prop_map,
                )

            _obj_param_map = seqr_wide_param_map
            if 'dataset' in labels:
                # specific override where 'dataset' is specified in GCP resource
                _obj_param_map = {labels['dataset']: (1.0, 1)}

            # Data transforms and key changes
            obj['topic'] = 'seqr'
            obj['service']['id'] = SERVICE_ID

            # reformat labels & system lables as string
            obj['labels'] = rapidjson.dumps(labels, sort_keys=True)
            obj['system_labels'] = rapidjson.dumps(
                utils.reformat_bigqquery_labels(obj['system_labels']),
                sort_keys=True,
            )

            nid = '-'.join([SERVICE_ID, 'seqr', billing_obj_to_key(obj)])
            obj['id'] = nid

            # For every seqr billing entry migrate it over
            _append_if_not_present(entries, obj)

            # For every seqr billing entry, add credit entry
            obj_credit = utils.get_credit(
                entry=obj,
                topic='seqr',
                project=utils.SEQR_PROJECT_FIELD,
            )
            _append_if_not_present(entries, obj_credit)

            for dataset, (ratio, dataset_size) in _obj_param_map.items():
                new_entry = copy.deepcopy(obj)

                new_entry['topic'] = dataset

                new_labels = copy.deepcopy(labels)
                new_labels['proportion'] = ratio
                new_labels['dataset_size'] = dataset_size

                new_entry['labels'] = rapidjson.dumps(new_labels, sort_keys=True)
                new_entry['cost'] *= ratio

                nid = '-'.join([SERVICE_ID, dataset, billing_obj_to_key(new_entry)])
                new_entry['id'] = nid

                _append_if_not_present(entries, new_entry)

        if mode == 'dry-run':
            result += len(entries)
        elif mode == 'prod':
            result += utils.upsert_rows_into_bigquery(
                table=utils.GCP_AGGREGATE_DEST_TABLE,
                objs=entries,
                existing_ids=existing_ids,
                dry_run=False,
            )
        elif mode == 'local':
            if not os.path.exists(output_path):
                os.mkdir(output_path)
            with open(
                os.path.join(
                    output_path,
                    f'seqr-hosting-{istart.isoformat()}-{iend.isoformat()}.json',
                ),
                'w+',
                encoding='utf-8',
            ) as f:
                # needs to be JSONL (line delimited JSON)
                f.writelines(rapidjson.dumps(e) + '\n' for e in entries)
            result += len(entries)

    return result


def billing_obj_to_key(obj: dict[str, Any]) -> str:
    """Convert a billing row to a hash which will be the row key"""
    identifier = hashlib.md5()  # noqa: S324
    identifier.update(rapidjson.dumps(obj, sort_keys=True).encode())
    return identifier.hexdigest()


# Proportionate map functions


async def generate_proportionate_maps_of_datasets(
    start: datetime,
    end: datetime,
    seqr_project_map: dict[str, int],
) -> tuple[ProportionateMapType, ProportionateMapType]:
    """
    Generate a proportionate map of datasets from list of samples
    in the relevant joint-calls (< 2022-06-01) or es-index (>= 2022-06-01)
    """

    projects = list(seqr_project_map.keys())

    # pylint: disable=too-many-locals
    sm_projects = await papi.get_all_projects_async()
    sm_pid_to_dataset = {p['id']: p['dataset'] for p in sm_projects}

    filtered_projects = list(set(sm_pid_to_dataset.values()).intersection(projects))
    missing_projects = set(projects) - set(sm_pid_to_dataset.values())
    if missing_projects:
        m = (
            f"The dataset(s) {', '.join(missing_projects)} were provided as "
            "'seqr-datasets' from metamist, but this account does not have access "
            "to read these project(s) ."
        )
        if any(p.endswith('-test') for p in missing_projects):
            m += (
                " Some of these datasets are 'test' projects, and this account does "
                "not have access to any test account."
            )
        raise ValueError(m)

    result = await aapi.get_proportionate_map_async(
        start=start.strftime('%Y-%m-%d'),
        body_get_proportionate_map=BodyGetProportionateMap(
            temporal_methods=[
                ProportionalDateTemporalMethod('SAMPLE_CREATE_DATE'),
                ProportionalDateTemporalMethod('ES_INDEX_DATE'),
            ],
            projects=filtered_projects,
            sequencing_types=[],
        ),
        end=end.strftime('%Y-%m-%d') if end else None,
    )

    def fix_types_in_result(res) -> list[tuple[date, dict[str, tuple[float, int]]]]:
        # received: list[{date: str, projects: list[{project, proportion, total}]}]
        # expected: list[tuple[datetime, dict[str, tuple[float, int]]]]
        return [
            (
                datetime.strptime(obj['date'], '%Y-%m-%d').date(),
                {
                    p['project']: (
                        float(p['percentage']),
                        int(p['size']),
                    )
                    for p in obj['projects']
                },
            )
            for obj in res
        ]

    seqr_hosting_map = fix_types_in_result(result['ES_INDEX_DATE'])
    shared_computation_map = fix_types_in_result(result['SAMPLE_CREATE_DATE'])

    return seqr_hosting_map, shared_computation_map


def get_ratios_from_date(
    dt: date,
    prop_map: ProportionateMapType,
) -> tuple[date, dict[str, tuple[float, int]]]:
    """
    From the prop_map, get the ratios for the applicable date.

    >>> get_ratios_from_date(
    ...     date(2023, 1, 1),
    ...     [(date(2020,12,31), {'d1': (1.0, 1)})]
    ... )
    (datetime.date(2020, 12, 31, 0, 0), {'d1': (1.0, 1)})

    >>> get_ratios_from_date(
    ...     date(2023, 1, 13),
    ...     [(date(2022,12,31), {'d1': (1.0, 1)}),
    ...      (date(2023,1,12), {'d1': (1.0, 2)})]
    ... )
    (datetime.date(2023, 1, 12, 0, 0), {'d1': (1.0, 2)})

    >>> get_ratios_from_date(
    ...     date(2023, 1, 3),
    ...     [(date(2023,1,2), {'d1': (1.0, 1)})]
    ... )
    (datetime.date(2023, 1, 2, 0, 0), {'d1': (1.0, 1)})

    >>> get_ratios_from_date(
    ...     date(2020, 1, 1),
    ...     [(date(2020,12,31), {'d1': (1.0, 1)})]
    ... )
    Traceback (most recent call last):
    ...
    AssertionError: No ratio found for date 2020-01-01
    """

    # prop_map is sorted ASC by date, so we
    # can find the latest element that is <= date
    for idt, m in prop_map[::-1]:
        # first entry BEFORE or EQUALS to the date
        if idt <= dt:
            return idt, m

    logger.error(dt)
    logger.error(prop_map)
    raise AssertionError(f'No ratio found for date {dt}')


# UTIL specific to seqr billing


def get_seqr_dataset_id_map() -> dict[str, int]:
    """
    Get Hail billing projects, same names as dataset
    """

    projects = papi.get_seqr_projects()
    return {x['name']: x['id'] for x in projects}


# DRIVER functions


async def main(
    start: datetime | None = None,
    end: datetime | None = None,
    mode: RunMode = 'prod',
    output_path: str | None = None,
    batch_ids: list[str] | None = None,
):
    """Main body function"""
    logger.info(f'Running Seqr Billing Aggregation for [{start}, {end}]')
    start, end = utils.process_default_start_and_end(start, end)

    seqr_project_map = get_seqr_dataset_id_map()

    @dataclasses.dataclass
    class PropMaps:
        """Class to hold proportionate maps so we don't need to think about references"""

        seqr_hosting_prop_map: ProportionateMapType | None = None
        shared_computation_prop_map: ProportionateMapType | None = None

    result = 0
    bq_output_path = None
    hail_output_path = None
    if output_path:
        bq_output_path = os.path.join(output_path, 'gcp')
        hail_output_path = os.path.join(output_path, 'hail')

        shutil.rmtree(output_path)
        for suffix in 'gcp', 'hail':
            os.makedirs(os.path.join(output_path, suffix), exist_ok=True)

    prop_maps = PropMaps()

    async def func_process_batches_to_fetch_prop_map(batches: list[dict]) -> list[dict]:
        """Just catch the batches loaded event to fetch the prop map"""
        time_created = [start] + [
            utils.parse_hail_time(b['time_created']) for b in batches
        ]
        min_time = min(time_created)

        time_completed = [end] + [
            utils.parse_hail_time(b['time_completed']) for b in batches
        ]
        max_time = max(time_completed)

        (
            seqr_hosting_prop_map,
            shared_computation_prop_map,
        ) = await generate_proportionate_maps_of_datasets(
            min_time,
            max_time,
            seqr_project_map=seqr_project_map,
        )

        prop_maps.seqr_hosting_prop_map = seqr_hosting_prop_map
        prop_maps.shared_computation_prop_map = shared_computation_prop_map

        return batches

    def func_get_finalised_entries(
        batch: utils.BatchType,
        jobs: list[utils.JobType],
    ) -> Generator[dict[str, Any], None, None]:
        return get_finalised_entries_for_batch(
            batch=batch,
            jobs=jobs,
            proportion_map=prop_maps.shared_computation_prop_map,
        )

    result += await utils.process_entries_from_hail_in_chunks(
        start=start,
        end=end,
        service_id=SERVICE_ID,
        billing_project=SEQR_HAIL_BILLING_PROJECT,
        func_get_finalised_entries_for_batch=func_get_finalised_entries,
        func_batches_preprocessor=func_process_batches_to_fetch_prop_map,
        mode=mode,
        output_path=hail_output_path,
        batch_ids=batch_ids,
    )

    result += migrate_entries_from_bq(
        start,
        end,
        prop_map=prop_maps.seqr_hosting_prop_map,
        mode=mode,
        output_path=bq_output_path,
    )

    if mode == 'dry-run':
        logger.info(f'Finished dry run, would have inserted {result} entries')
    elif mode == 'local':
        logger.info(f'Wrote {result} entries to local disk for inspection')
    else:
        logger.info(f'Inserted {result} entries')

    return {'entriesInserted': result}


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


if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('google.auth.compute_engine._metadata').setLevel(logging.ERROR)
    logging.getLogger('asyncio').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    test_start, test_end = None, None

    asyncio.new_event_loop().run_until_complete(
        main(
            start=test_start,
            end=test_end,
            mode='prod',
            # output_path=os.path.join(os.getcwd(), 'seqr'),
        ),
    )
