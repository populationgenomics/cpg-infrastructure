# pylint: disable=global-statement,too-many-arguments,line-too-long,too-many-lines
"""
Class of helper functions for billing aggregate functions
"""
import os
import re
import sys
import math
import json
import asyncio
import logging

from io import StringIO
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Any, Iterator, Sequence, TypeVar, Iterable, Optional, Union

import aiohttp
import pandas as pd
import google.cloud.logging
import google.cloud.bigquery as bq
import rapidjson

from cpg_utils.cloud import read_secret
from google.api_core.exceptions import ClientError
from pandas import Timestamp

for lname in (
    'asyncio',
    'urllib3',
    'google',
):
    logging.getLogger(lname).setLevel(logging.WARNING)

logging.basicConfig()

if os.getenv('SETUP_GCP_LOGGING'):
    client = google.cloud.logging.Client()
    client.setup_logging()

logger = logging.getLogger('cost-aggregate')
logger.setLevel(logging.INFO)
# logger.propagate = False

if os.getenv('DEBUG') in ('1', 'true', 'yes') or os.getenv('DEV') in (
    '1',
    'true',
    'yes',
):
    logger.setLevel(logging.INFO)

# pylint: disable=invalid-name
T = TypeVar('T')

DEFAULT_TOPIC = 'admin'

GCP_PROJECT = os.getenv('BILLING_PROJECT_ID')
GCP_BILLING_BQ_TABLE = os.getenv('GCP_BILLING_SOURCE_TABLE')
GCP_AGGREGATE_DEST_TABLE = os.getenv('GCP_AGGREGATE_DEST_TABLE')

assert GCP_AGGREGATE_DEST_TABLE
logger.info(f'GCP_AGGREGATE_DEST_TABLE: {GCP_AGGREGATE_DEST_TABLE}')

IS_PRODUCTION = os.getenv('PRODUCTION') in ('1', 'true', 'yes')

# mfranklin 2022-07-25: dropping to 0% service-fee.
HAIL_SERVICE_FEE = 0.0
# BQ only allows 10,000 parameters in a query, so given the way we upsert rows,
# only upsert DEFAULT_BQ_INSERT_CHUNK_SIZE at once:
# https://cloud.google.com/bigquery/quotas#:~:text=up%20to%2010%2C000%20parameters.
DEFAULT_BQ_INSERT_CHUNK_SIZE = 20000
ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'

# runs every 4 hours
DEFAULT_RANGE_INTERVAL = timedelta(hours=int(os.getenv('DEFAULT_INTERVAL_HOURS', '4')))

SEQR_PROJECT_ID = 'seqr-308602'
HAIL_PROJECT_ID = 'hail-295901'

HAIL_BASE = 'https://batch.hail.populationgenomics.org.au'
HAIL_UI_URL = HAIL_BASE + '/batches/{batch_id}'
HAIL_BATCHES_API = HAIL_BASE + '/api/v1alpha/batches'
HAIL_JOBS_API = HAIL_BASE + '/api/v1alpha/batches/{batch_id}/jobs/resources'

HAIL_PROJECT_FIELD = {
    'id': HAIL_PROJECT_ID,
    'number': '805950571114',
    'name': HAIL_PROJECT_ID,
    'labels': [],
    'ancestry_numbers': '/648561325637/',
    'ancestors': [
        {
            'resource_name': 'projects/805950571114',
            'display_name': HAIL_PROJECT_ID,
        },
        {
            'resource_name': 'organizations/648561325637',
            'display_name': 'populationgenomics.org.au',
        },
    ],
}

SEQR_PROJECT_FIELD = {
    'id': SEQR_PROJECT_ID,
    'number': '1021400127367',
    'name': SEQR_PROJECT_ID,
    'labels': [],
    'ancestry_numbers': '/648561325637/',
    'ancestors': [
        {
            'resource_name': 'organizations/648561325637',
            'display_name': 'populationgenomics.org.au',
        }
    ],
}


_BQ_CLIENT: bq.Client | None = None


def get_bigquery_client():
    """Get instantiated cached bq client"""
    global _BQ_CLIENT
    if not _BQ_CLIENT:
        _BQ_CLIENT = bq.Client()
    return _BQ_CLIENT


async def async_retry_transient_get_json_request(
    url,
    errors: Union[Exception, tuple[Exception, ...]],
    *args,
    attempts=5,
    session=None,
    timeout_seconds=60,
    **kwargs,
):
    """
    Retry a function with exponential backoff.
    """

    async def inner_block(session):
        for attempt in range(1, attempts + 1):
            try:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=timeout_seconds),
                    *args,
                    **kwargs,
                ) as resp:
                    resp.raise_for_status()
                    j = await resp.json()
                    return j
            # pylint: disable=broad-except
            except Exception as e:
                if not isinstance(e, errors):
                    raise
                if attempt == attempts:
                    raise

            t = 2 ** (attempt + 1)
            logger.warning(f'Backing off {t} seconds for {url}')
            await asyncio.sleep(t)

    if session:
        return await inner_block(session)

    async with aiohttp.ClientSession() as session2:
        return await inner_block(session2)


def chunk(iterable: Sequence[T], chunk_size) -> Iterator[Sequence[T]]:
    """
    Chunk a sequence by yielding lists of `chunk_size`
    """
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


def get_total_hail_cost(currency_conversion_rate, raw_cost):
    """Get cost from hail batch_resource, including SERVICE_FEE"""

    return (1 + HAIL_SERVICE_FEE) * currency_conversion_rate * raw_cost


def get_schema_json(file: str) -> dict[str, any]:
    """Get a schema (in JSON) from the schema directory"""
    pwd = Path(__file__).parent.parent.resolve()
    schema_path = pwd / 'schema' / file
    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as exp:
        raise exp


def get_bq_schema_json() -> dict[str, any]:
    """Get the bq schema (in JSON) for the aggregate table"""
    return get_schema_json('aggregate_schema.json')


def _format_bq_schema_json(schema: dict[str, Any]):
    """
    Take bq json schema, and convert it to bq.SchemaField objects"""
    formatted_schema = []
    for row in schema:
        kwargs = {
            'name': row['name'],
            'field_type': row['type'],
            'mode': row['mode'],
        }

        if 'fields' in row and row['fields']:
            kwargs['fields'] = _format_bq_schema_json(row['fields'])
        formatted_schema.append(bq.SchemaField(**kwargs))
    return formatted_schema


def get_formatted_bq_schema() -> list[bq.SchemaField]:
    """
    Get schema for bigquery billing table, as a list of bq.SchemaField objects
    """
    return _format_bq_schema_json(get_bq_schema_json())


def parse_date_only_string(d: Optional[str]) -> Optional[date]:
    """Convert date string to date, allow for None"""
    if not d:
        return None

    try:
        return datetime.strptime(d, '%Y-%m-%d').date()
    except Exception as excep:
        raise ValueError(f'Date could not be converted: {d}') from excep


def parse_hail_time(time_str: str) -> datetime:
    """
    Parse hail datetime object

    >>> parse_hail_time('2022-06-09T04:59:58Z').isoformat()
    '2022-06-09T04:59:58'
    """
    if isinstance(time_str, datetime):
        return time_str

    if time_str is None:
        return None

    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ'):
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            pass

    raise ValueError(f'Could not convert date {time_str}')


def to_bq_time(time: datetime):
    """Convert datetime to transport datetime for bigquery"""
    return time.strftime('%Y-%m-%d %H:%M:%S')


def get_date_time_from_value(key, value):
    """
    Guess datetime from some value
    """
    if isinstance(value, Timestamp):
        return value.to_pydatetime()
    if isinstance(value, str):
        if value.isdigit():
            value = int(value)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(int(value / 1000))

    raise ValueError(
        f'Unable to determine {key} datetime conversion format: {value} :: {type(value)}'
    )


def get_hail_token() -> str:
    """
    Get Hail token from local tokens file
    """
    if os.getenv('DEV') in ('1', 'true', 'yes'):
        with open(os.path.expanduser('~/.hail/tokens.json'), encoding='utf-8') as f:
            config = json.load(f)
            return config['default']

    assert GCP_PROJECT
    return read_secret(
        GCP_PROJECT, 'aggregate-billing-hail-token', fail_gracefully=False
    )


def get_credits(
    entries: Iterable[dict[str, Any]],
    topic: str,
    project: dict,
) -> list[dict[str, any]]:
    """
    Get a hail / seqr credit for each entry.

    Dependent on where the cost should be attributed, we apply a 'credit'
    to that topic in order to balanace where money is spent. For example,
    say $DATASET runs a job using Hail. We determine the cost of that job,
    apply a 'debit' to $DATASET, and an equivalent 'credit' to Hail.

    The rough idea being the Hail topic should be roughly $0,
    minus adminstrative overhead.
    """

    hail_credits = [{**e} for e in entries]
    for entry in hail_credits:
        entry['topic'] = topic
        entry['id'] += '-credit'
        entry['cost'] = -entry['cost']
        entry['service'] = {
            **entry['service'],
            'description': entry['service']['description'] + ' Credit',
        }
        sku = {**entry['sku']}
        sku['id'] += '-credit'
        sku['description'] += '-credit'
        entry['sku'] = sku
        entry['project'] = project

    return hail_credits


async def get_batches(
    token: str,
    billing_project: Optional[str] = None,
    last_batch_id: Optional[Any] = None,
) -> dict[str, any]:
    """
    Get list of batches for a billing project with no filtering.
    (optional): billing_project
        If no billing_project is set, this endpoint returns batches
        from all BPs the user (aggregate-billing) is a part of.
    (optional): last_batch_id (found in requests)
    """

    qparams = {}
    params = []
    if billing_project:
        qparams['billing_project'] = billing_project
    else:
        params = ['q=']

    params.extend(f'q={k}:{v}' for k, v in qparams.items())
    if last_batch_id:
        params.append(f'last_batch_id={last_batch_id}')

    q = '?' + '&'.join(params)
    url = HAIL_BATCHES_API + q

    logger.debug(f'Getting batches: {url}')

    return await async_retry_transient_get_json_request(
        url,
        aiohttp.ClientError,
        headers={'Authorization': 'Bearer ' + token},
    )


async def get_finished_batches_for_date(
    start: datetime,
    end: datetime,
    token: str,
    billing_project: Optional[str] = None,
) -> list[dict[str, any]]:
    """
    Get all the batches that started on {date} and are complete.
    We assume that batches are ordered by start time, so we can stop
    when we find a batch that started before the date.
    """
    batches = []
    last_batch_id = None
    n_requests = 0
    skipped = 0

    logger.info(f'Getting batches for range: [{start}, {end}]')

    while True:
        n_requests += 1
        jresponse = await get_batches(
            billing_project=billing_project, last_batch_id=last_batch_id, token=token
        )

        if 'last_batch_id' in jresponse and jresponse['last_batch_id'] == last_batch_id:
            raise ValueError(
                f'Something weird is happening with last_batch_job: {last_batch_id}'
            )
        last_batch_id = jresponse.get('last_batch_id')
        if not jresponse.get('batches'):
            logger.error(f'No batches found for range: [{start}, {end}]')
            return batches
        for b in jresponse['batches']:
            # batch not finished or not finished within the (start, end) range
            if not b['time_completed'] or not b['complete']:
                skipped += 1
                continue

            time_completed = parse_hail_time(b['time_completed'])
            in_date_range = start <= time_completed < end

            if time_completed < start:
                logger.info(
                    f'{billing_project} :: Got {len(batches)} batches '
                    f'in {n_requests} requests, skipping {skipped}'
                )
                return batches
            if in_date_range:
                batches.append(b)
            else:
                skipped += 1


async def get_jobs_for_batch(batch_id, token: str) -> list[dict[str, any]]:
    """
    For a single batch, fill in the 'jobs' field.
    """
    jobs = []
    last_job_id = None
    end = False
    iterations = 0

    async with aiohttp.ClientSession() as session:
        while not end:
            iterations += 1

            if iterations > 1 and iterations % 5 == 0:
                logger.info(f'On {iterations} iteration to load jobs for {batch_id}')

            q = '?limit=9999'
            if last_job_id:
                q += f'&last_job_id={last_job_id}'
            url = HAIL_JOBS_API.format(batch_id=batch_id) + q

            jresponse = await async_retry_transient_get_json_request(
                url,
                aiohttp.ClientError,
                session=session,
                headers={'Authorization': 'Bearer ' + token},
            )
            new_last_job_id = jresponse.get('last_job_id')
            if new_last_job_id is None:
                end = True
            elif last_job_id:
                assert new_last_job_id > last_job_id
            last_job_id = new_last_job_id
            jobs.extend(jresponse['jobs'])

    return jobs


async def process_entries_from_hail_in_chunks(
    start: datetime,
    end: datetime,
    func_get_finalised_entries_for_batch,
    billing_project: str = None,
    entry_chunk_size=500,
    batch_group_chunk_size=30,
    log_prefix: str = '',
    mode: str = 'prod',
    output_path: str = './',
) -> int:
    """
    Process all the seqr entries from hail batch,
    and insert them into the aggregate table.

    Break them down by dataset, and then proportion the rest of the costs.
    """

    def insert_entries(entries):
        if mode in ('prod', 'dry-run'):
            return upsert_rows_into_bigquery(
                table=GCP_AGGREGATE_DEST_TABLE, objs=entries, dry_run=mode == 'dry-run'
            )

        if mode == 'local':
            counter = 1
            filename = os.path.join(output_path, f'processed-hail-{counter}.json')
            while os.path.exists(filename):
                counter += 1
                filename = os.path.join(output_path, f'processed-hail-{counter}.json')
            with open(filename, 'w+', encoding='utf-8') as file:
                logger.info(f'Writing {len(entries)} to {filename}')
                # needs to be JSONL (line delimited JSON)
                file.writelines(rapidjson.dumps(e) + '\n' for e in entries)

            return len(entries)

        raise ValueError(f'Invalid mode: {mode}')

    # pylint: disable=too-many-locals
    token = get_hail_token()
    result = 0
    lp = f'{log_prefix} ::' if log_prefix else ''

    batches = await get_finished_batches_for_date(
        start=start, end=end, token=token, billing_project=billing_project
    )
    if len(batches) == 0:
        return 0

    chunk_counter = 0
    nchnks = math.ceil(len(batches) / entry_chunk_size) * batch_group_chunk_size

    # Process chunks of batches to avoid loading too many entries into memory
    for batch_group in chunk(batches, entry_chunk_size):
        jobs_in_batch = []
        entries = []

        # Get jobs for a fraction of each chunked batches
        # to avoid hitting hail batch too much
        for chunked_batch_Group in chunk(batch_group, batch_group_chunk_size):
            chunk_counter += 1
            times = [b['time_created'] for b in chunked_batch_Group]
            min_batch = min(times)
            max_batch = max(times)

            if len(batches) > 100:
                logger.debug(
                    f'{lp}Getting jobs for batch chunk {chunk_counter}/{nchnks} '
                    f'[{min_batch}, {max_batch}]'
                )

            promises = [get_jobs_for_batch(b['id'], token) for b in chunked_batch_Group]
            jobs_in_batch.extend(await asyncio.gather(*promises))

        # insert all entries for each batch
        for batch, jobs in zip(batch_group, jobs_in_batch):
            batch['jobs'] = jobs
            if len(jobs) > 10000 and len(entries) > 1000:
                logger.info(
                    f'Expecting large number of jobs ({len(jobs)}) from '
                    f"batch {batch['id']}, inserting contents early"
                )
                result += insert_entries(entries)
                entries = []

            entries_for_batch = func_get_finalised_entries_for_batch(batch)
            entries.extend(entries_for_batch)

            s = sum(sys.getsizeof(e) for e in entries) / 1024 / 1024
            if s > 10:
                logger.info(f'Size of entries: {s} MB, inserting early')
                result += insert_entries(entries)
                entries = []

        result += insert_entries(entries)
    return result


RE_matcher = re.compile(r'-\d+$')


def billing_row_to_topic(row, dataset_to_gcp_map: dict) -> Optional[str]:
    """Convert a billing row to a topic name"""
    project_id = None

    if project := row['project']:
        assert isinstance(project, dict)
        project_id = project.get('id')

    topic = dataset_to_gcp_map.get(project_id, project_id)

    # Default topic, any cost not clearly associated with a project will be considered
    # overhead administrative costs. This category should be minimal
    if not topic:
        return DEFAULT_TOPIC

    topic = RE_matcher.sub('', topic)
    return topic


def upsert_rows_into_bigquery(
    objs: list[dict[str, Any]],
    dry_run: bool,
    table: str = GCP_AGGREGATE_DEST_TABLE,
    chunk_size=DEFAULT_BQ_INSERT_CHUNK_SIZE,
    max_chunk_size_mb=6,
) -> int:
    """
    Upsert JSON rows into the BQ.aggregate table.
    It must respect the schema defined in get_bq_schema_json().

    This method will chunk the list of objects into upsertable chunks
    check which chunks are already in the table, and insert any
    that are not present.

    It has some optimisations about max insert size, so this should be
    able to take an arbitrary amount of rows.
    """

    if not objs:
        logger.info('Not inserting any rows')
        return 0

    n_chunks = math.ceil(len(objs) / chunk_size)
    total_size_mb = sys.getsizeof(objs) / (1024 * 1024)

    # if average_chunk_size > max_chunk_size
    if (total_size_mb / n_chunks) > max_chunk_size_mb:
        # bigger than max_chunk_size, so let's reduce it
        chunk_size = math.ceil(total_size_mb / max_chunk_size_mb)
        n_chunks = math.ceil(len(objs) / chunk_size)

        logger.info(
            'The size of the objects to insert into BQ is too large, '
            f'adjusting the chunk size to {chunk_size}'
        )

    if n_chunks > 1:
        logger.info(f'Will insert {len(objs)} rows in {n_chunks} chunks')

    inserts = 0
    inserted_ids = set()

    for chunk_idx, chunked_objs in enumerate(chunk(objs, chunk_size)):
        _query = f"""
            SELECT id FROM `{table}`
            WHERE id IN UNNEST(@ids);
        """

        # NOTE: it's possible to have valid duplicate rows
        # allow for adding duplicates on first upload only
        # Protects us against duplicate ids falling across chunks
        ids = set(o['id'] for o in chunked_objs) - inserted_ids

        job_config = bq.QueryJobConfig(
            query_parameters=[
                bq.ArrayQueryParameter('ids', 'STRING', list(ids)),
            ]
        )

        result = get_bigquery_client().query(_query, job_config=job_config).result()
        existing_ids = set(result.to_dataframe()['id'])

        # Filter out any rows that are already in the table
        filtered_obj = [o for o in chunked_objs if o['id'] not in existing_ids]

        nrows = len(filtered_obj)

        if nrows == 0:
            logger.info(
                f'Not inserting any rows 0/{len(chunked_objs)} '
                f'({chunk_idx+1}/{n_chunks} chunk)'
            )
            continue

        if dry_run:
            logger.info(
                f'DRY_RUN: Inserting {nrows}/{len(chunked_objs)} rows '
                f'({chunk_idx+1}/{n_chunks} chunk)'
            )
            inserts += nrows
            continue

        # Count number of rows adding
        logger.info(
            f'Inserting {nrows}/{len(chunked_objs)} rows '
            f'({chunk_idx+1}/{n_chunks} chunk)'
        )

        # Insert the new rows
        job_config = bq.LoadJobConfig()
        job_config.source_format = bq.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = get_formatted_bq_schema()

        j = '\n'.join(json.dumps(o) for o in filtered_obj)

        resp = get_bigquery_client().load_table_from_file(
            StringIO(j), table, job_config=job_config, project=GCP_PROJECT
        )
        try:
            result = resp.result()
            logger.info(
                f'Inserted {result.output_rows}/{nrows} rows '
                f'({chunk_idx+1}/{n_chunks} chunk)'
            )
        except ClientError as e:
            logger.error(resp.errors)
            raise e

        inserts += nrows
        inserted_ids = inserted_ids.union(ids)

    return inserts


def upsert_aggregated_dataframe_into_bigquery(
    df: pd.DataFrame, table: str = GCP_AGGREGATE_DEST_TABLE
):
    """
    Upsert rows from a dataframe into the BQ.aggregate table.
    It must respect the schema defined in get_bq_schema_json().
    """

    if len(df['id']) == 0:
        logger.info(f'No rows to insert')
        return 0

    # Cannot use query parameters for table names
    # https://cloud.google.com/bigquery/docs/parameterized-queries
    _query = f"""
        SELECT id FROM {table}
        WHERE id IN UNNEST(@ids);
    """
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ArrayQueryParameter('ids', 'STRING', list(set(df['id']))),
        ]
    )

    result = get_bigquery_client().query(_query, job_config=job_config).result()
    existing_ids = set(result.to_dataframe()['id'])

    # Filter out any rows that are already in the table
    df = df[~df['id'].isin(existing_ids)]

    # Count number of rows adding
    adding_rows = len(df)

    # Insert the new rows
    project_id = table.split('.')[0]

    table_schema = get_bq_schema_json()
    df.to_gbq(
        table,
        project_id=project_id,
        table_schema=table_schema,
        if_exists='append',
        chunksize=DEFAULT_BQ_INSERT_CHUNK_SIZE,
    )

    logger.info(f'{adding_rows} new rows inserted')
    return adding_rows


CACHED_CURRENCY_CONVERSION: dict[str, float] = {}


def get_currency_conversion_rate_for_time(time: datetime):
    """
    Get the currency conversion rate for a given time.
    Noting that GCP conversion rates are decided at the start of the month,
    and apply to each job that starts within the month, regardless of when
    the job finishes.
    """

    key = f'{time.year}-{str(time.month).zfill(2)}'
    if key not in CACHED_CURRENCY_CONVERSION:
        logger.info(f'Looking up currency conversion rate for {key}')
        query = f"""
            SELECT currency_conversion_rate
            FROM {GCP_BILLING_BQ_TABLE}
            WHERE DATE(_PARTITIONTIME) = DATE('{time.date()}')
            LIMIT 1
        """
        query_result = get_bigquery_client().query(query).result()

        if query_result.total_rows == 0:
            raise ValueError(
                f'Could not find billing data for {key!r}, for {time.date()}'
            )

        for r in query_result:
            CACHED_CURRENCY_CONVERSION[key] = r['currency_conversion_rate']

    return CACHED_CURRENCY_CONVERSION[key]


def get_unit_for_batch_resource_type(batch_resource_type: str) -> str:
    """
    Get the relevant unit for some hail batch resource type
    """
    return {
        'boot-disk/pd-ssd/1': 'mib * msec',
        'disk/local-ssd/preemptible/1': 'mib * msec',
        'disk/local-ssd/nonpreemptible/1': 'mib * msec',
        'disk/local-ssd/1': 'mib * msec',
        'disk/pd-ssd/1': 'mb * msec',
        'compute/n1-nonpreemptible/1': 'mcpu * msec',
        'compute/n1-preemptible/1': 'mcpu * msec',
        'ip-fee/1024/1': 'IP * msec',
        'memory/n1-nonpreemptible/1': 'mib * msec',
        'memory/n1-preemptible/1': 'mib * msec',
        'service-fee/1': '$/msec',
    }.get(batch_resource_type, batch_resource_type)


def get_start_and_end_from_request(
    request,
) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Get the start and end times from the cloud function request.
    """
    if request:
        print(request)
        # return request.params['start'], request.params['end']
    return None, None


def date_range_iterator(
    start,
    end,
    intv=DEFAULT_RANGE_INTERVAL,
) -> Iterator[tuple[datetime, datetime]]:
    """
    Iterate over a range of dates.

    >>> list(date_range_iterator(datetime(2019, 1, 1), datetime(2019, 1, 2), intv=timedelta(days=2)))
    [(datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2019, 1, 2, 0, 0))]

    >>> list(date_range_iterator(datetime(2019, 1, 1), datetime(2019, 1, 3), intv=timedelta(days=2)))
    [(datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2019, 1, 3, 0, 0))]

    >>> list(date_range_iterator(datetime(2019, 1, 1), datetime(2019, 1, 4), intv=timedelta(days=2)))
    [(datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2019, 1, 3, 0, 0)), (datetime.datetime(2019, 1, 3, 0, 0), datetime.datetime(2019, 1, 4, 0, 0))]

    """  # noqa: E501
    dt_from = start
    dt_to = start + intv
    while dt_to < end:
        yield dt_from, dt_to
        dt_from += intv
        dt_to += intv

    dt_to = min(dt_to, end)
    if dt_from < dt_to:
        yield dt_from, dt_to


def get_start_and_end_from_data(data) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Get the start and end times from the cloud function data.
    """
    if data is not None:
        # Convert str to json
        if isinstance(data, str):
            try:
                data = dict(json.loads(data))
            except ValueError:
                return (None, None)

        # Extract date attributes from dict
        dates = {}
        if data.get('attributes'):
            dates = data.get('attributes', {})
        elif data.get('start') or data.get('end'):
            dates = data
        elif data.get('message'):
            try:
                return get_start_and_end_from_data(data['message'])
            except ValueError:
                dates = {}

        logger.info(f'data: {data}, dates: {dates}')

        s_raw = dates.get('start')
        e_raw = dates.get('end')

        # this should except if the start/end is in an invalid format
        start = datetime.fromisoformat(s_raw) if s_raw else None
        end = datetime.fromisoformat(e_raw) if e_raw else None

        return (start, end)

    return (None, None)


def process_default_start_and_end(
    start: Optional[datetime],
    end: Optional[datetime],
    interval: timedelta = DEFAULT_RANGE_INTERVAL,
) -> tuple[datetime, datetime]:
    """
    Take input start / end values, and apply
    defaults
    """
    if not end:
        # start right now
        end = datetime.now()
    if not start:
        start = end - interval

    assert isinstance(start, datetime) and isinstance(end, datetime)
    return start, end


def get_date_intervals_for(
    start: Optional[datetime],
    end: Optional[datetime],
    interval: timedelta = DEFAULT_RANGE_INTERVAL,
) -> Iterator[tuple[datetime, datetime]]:
    """
    Process start and end times from source (by adding appropriate defaults)
    and return a date_range iterator based on the interval.
    """
    s, e = process_default_start_and_end(start, end)
    return date_range_iterator(s, e, intv=interval)


def get_hail_entry(
    key: str,
    topic: str,
    service_id: str,
    description: str,
    cost: float,
    currency_conversion_rate: float,
    usage: float,
    batch_resource: str,
    start_time: datetime,
    end_time: datetime,
    labels: dict[str, str] = None,
) -> dict[str, Any]:
    """
    Get well formed entry dictionary from keys
    """

    assert labels is None or isinstance(labels, dict)

    _labels = []
    if labels:
        _labels = [
            {'key': k, 'value': str(v).encode('ascii', 'ignore').decode()}
            for k, v in labels.items()
        ]
    return {
        'id': key,
        'topic': topic,
        'service': {'id': service_id, 'description': description},
        'sku': {
            'id': f'hail-{batch_resource}',
            'description': batch_resource,
        },
        'usage_start_time': to_bq_time(start_time),
        'usage_end_time': to_bq_time(end_time),
        'project': None,
        'labels': _labels,
        'system_labels': [],
        'location': {
            'location': 'australia-southeast1',
            'country': 'Australia',
            'region': 'australia',
            'zone': None,
        },
        'export_time': to_bq_time(datetime.now()),
        'cost': cost,
        'currency': 'AUD',
        'currency_conversion_rate': currency_conversion_rate,
        'usage': {
            'amount': usage,
            'unit': get_unit_for_batch_resource_type(batch_resource),
            'amount_in_pricing_units': cost,
            'pricing_unit': 'AUD',
        },
        'credits': [],
        'invoice': {'month': f'{start_time.year}{str(start_time.month).zfill(2)}'},
        'cost_type': 'regular',
        'adjustment_info': None,
    }


if __name__ == '__main__':
    json_str = "{'start': '2022-01-01', 'end': '2022-01-02'}"
    json_strt, json_end = datetime.fromisoformat('2022-01-01'), datetime.fromisoformat(
        '2022-01-02'
    )
    get_start_and_end_from_data({'message': json_str})