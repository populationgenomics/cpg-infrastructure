# flake8: noqa: PLR2004,ERA001,DTZ003,DTZ005,DTZ006,DTZ007,C901,ANN401
"""
Class of helper functions for billing aggregate functions
"""
import asyncio
import json
import logging
import math
import os
import re
import sys
from base64 import b64decode
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    Iterator,
    Literal,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

import aiohttp
import google.cloud.bigquery as bq
import google.cloud.logging
import pandas as pd
import rapidjson
from cpg_utils.cloud import read_secret
from flask import Request
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

INVOICE_DAY_DIFF = 3

GCP_PROJECT = os.getenv('BILLING_PROJECT_ID')
if GCP_PROJECT:
    os.environ['GOOGLE_CLOUD_PROJECT'] = GCP_PROJECT

GCP_BILLING_BQ_TABLE = os.getenv('GCP_BILLING_SOURCE_TABLE')
GCP_AGGREGATE_DEST_TABLE = os.getenv('GCP_AGGREGATE_DEST_TABLE')

assert GCP_AGGREGATE_DEST_TABLE
logger.info(f'GCP_AGGREGATE_DEST_TABLE: {GCP_AGGREGATE_DEST_TABLE}')

IS_PRODUCTION = os.getenv('PRODUCTION') in ('1', 'true', 'yes')

BatchType = dict[str, Any]
JobType = dict[str, Any]

# mfranklin 2022-07-25: dropping to 0% service-fee.
HAIL_SERVICE_FEE = 0.0
# BQ only allows 10,000 parameters in a query, so given the way we upsert rows,
# only upsert DEFAULT_BQ_INSERT_CHUNK_SIZE at once:
# https://cloud.google.com/bigquery/quotas#:~:text=up%20to%2010%2C000%20parameters.
DEFAULT_BQ_INSERT_CHUNK_SIZE = 20000
ANALYSIS_RUNNER_PROJECT_ID = 'analysis-runner'

# Maximum job count before all jobs gets summarised into one row
# This is to prevent the bigquery inserts taking too long
# Most of batches with over 20K jobs are hail query jobs and are not very useful for billing
DEFAULT_MAX_JOBS_PER_BATCH = 9000

# runs every 4 hours
DEFAULT_RANGE_INTERVAL = timedelta(hours=int(os.getenv('DEFAULT_INTERVAL_HOURS', '4')))

SEQR_PROJECT_ID = 'seqr-308602'
ES_INDEX_PROJECT_ID = 'pr418c6531826c4cae'
HAIL_PROJECT_ID = 'hail-295901'

HAIL_BASE = 'https://batch.hail.populationgenomics.org.au'
HAIL_UI_URL = HAIL_BASE + '/batches/{batch_id}'
HAIL_BATCHES_API = HAIL_BASE + '/api/v1alpha/batches/completed'
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
        },
    ],
}


_BQ_CLIENT: bq.Client | None = None


def get_bigquery_client():
    """Get instantiated cached bq client"""
    global _BQ_CLIENT
    if not _BQ_CLIENT:
        assert GCP_PROJECT
        _BQ_CLIENT = bq.Client(project=GCP_PROJECT)
    return _BQ_CLIENT


async def async_retry_transient_get_json_request(
    url: str,
    errors: Type[Exception] | tuple[Type[Exception], ...],
    *args: list[Any],
    attempts: int = 5,
    session: aiohttp.ClientSession | None = None,
    timeout_seconds: int = 60,
    **kwargs: dict[str, Any],
) -> T:
    """
    Retry a function with exponential backoff.
    """

    async def inner_block(_session: aiohttp.ClientSession) -> T:
        last_exception = None
        for attempt in range(1, attempts + 1):
            try:
                async with _session.get(
                    url,
                    *args,
                    timeout=aiohttp.ClientTimeout(total=timeout_seconds),
                    **kwargs,
                ) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            # pylint: disable=broad-except
            except Exception as e:  # noqa: BLE001
                last_exception = e
                if not isinstance(e, errors):
                    raise
                if attempt == attempts:
                    raise

            t = 2 ** (attempt + 1)
            logger.warning(f'Backing off {t} seconds due to {last_exception} for {url}')
            await asyncio.sleep(t)

        raise Exception(f'No attempt suceeded for {url}, and no exception was raised')

    if session:
        return await inner_block(session)

    async with aiohttp.ClientSession() as session2:
        return await inner_block(session2)


def chunk(iterable: Sequence[T], chunk_size: int) -> Iterator[Sequence[T]]:
    """
    Chunk a sequence by yielding lists of `chunk_size`
    """
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i : i + chunk_size]


def get_total_hail_cost(currency_conversion_rate: float, raw_cost: float):
    """Get cost from hail batch_resource, including SERVICE_FEE"""

    return (1 + HAIL_SERVICE_FEE) * currency_conversion_rate * raw_cost


def get_schema_json(file: str) -> list[dict[str, Any]]:
    """Get a schema (in JSON) from the schema directory"""
    pwd = Path(__file__).parent.resolve()
    schema_path = pwd / file
    try:
        with open(schema_path, encoding='utf-8') as f:
            return json.load(f)
    except Exception as exp:
        raise exp


def get_bq_schema_json() -> list[dict[str, Any]]:
    """Get the bq schema (in JSON) for the aggregate table"""
    return get_schema_json('aggregate_schema.json')


def _format_bq_schema_json(schema: list[dict[str, Any]]) -> list[dict]:
    """
    Take bq json schema, and convert it to bq.SchemaField objects"""
    formatted_schema = []
    for row in schema:
        kwargs = {
            'name': row['name'],
            'field_type': row['type'],
            'mode': row['mode'],
        }

        if fields := row.get('fields'):
            kwargs['fields'] = _format_bq_schema_json(fields)
        formatted_schema.append(bq.SchemaField(**kwargs))
    return formatted_schema


def get_formatted_bq_schema() -> list[bq.SchemaField]:
    """
    Get schema for bigquery billing table, as a list of bq.SchemaField objects
    """
    return _format_bq_schema_json(get_bq_schema_json())


def parse_date_only_string(d: str | None) -> date | None:
    """Convert date string to date, allow for None"""
    if not d:
        return None

    try:
        return datetime.strptime(d, '%Y-%m-%d').date()
    except Exception as excep:  # noqa: BLE001
        raise ValueError(f'Date could not be converted: {d}') from excep


def parse_hail_time(time_str: str) -> datetime:
    """
    Parse hail datetime object

    >>> parse_hail_time('2022-06-09T04:59:58Z').isoformat()
    '2022-06-09T04:59:58'
    """
    if isinstance(time_str, datetime):
        return time_str

    if not time_str:
        raise ValueError(f'Could not convert date, time_str has no value: {time_str!r}')

    exceptions = []
    if time_str.endswith('Z'):
        # the fromisoformat method doesn't like the Z at the end
        # so we remove it and add the offset to make a offset-aware datetime
        time_str = time_str[:-1]
        _time_str = time_str + '+00:00'
        try:
            return datetime.fromisoformat(_time_str)
        except ValueError as e:
            exceptions.append(e)

    try:
        fmt = "%Y-%m-%dT%H:%M:%S"
        return datetime.strptime(time_str, fmt).replace(tzinfo=timezone.utc)
    except ValueError as e:
        exceptions.append(e)

    raise ValueError(f'Could not convert date {time_str}: {exceptions}')


def to_bq_time(time: datetime):
    """Convert datetime to transport datetime for bigquery"""
    return time.strftime('%Y-%m-%d %H:%M:%S')


def get_date_time_from_value(key: str, value: Any) -> datetime:
    """
    Guess datetime from some value
    """
    if isinstance(value, Timestamp):
        return value.to_pydatetime()
    if isinstance(value, str) and value.isdigit():
        value = int(value)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(int(value / 1000))

    raise ValueError(
        f'Unable to determine {key} datetime conversion format: {value} :: {type(value)}',
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
    secret_value = read_secret(
        GCP_PROJECT,
        'aggregate-billing-hail-token',
        fail_gracefully=False,
    )
    if not secret_value:
        raise ValueError('Could not find Hail token')

    return secret_value


def get_credit(entry: dict[str, Any], topic: str, project: dict[str, Any]):
    """
    Dependent on where the cost should be attributed, we apply a 'credit'
    to that topic in order to balanace where money is spent. For example,
    say $DATASET runs a job using Hail. We determine the cost of that job,
    apply a 'debit' to $DATASET, and an equivalent 'credit' to Hail.

    The rough idea being the Hail topic should be roughly $0,
    minus adminstrative overhead.

    """
    _entry = entry.copy()
    _entry['topic'] = topic
    _entry['id'] += '-credit'
    _entry['cost'] = -entry['cost']
    _entry['service'] = {
        **_entry['service'],
        'description': entry['service']['description'] + ' Credit',
    }
    sku = {**_entry['sku']}
    sku['id'] += '-credit'
    sku['description'] += '-credit'
    _entry['sku'] = sku
    _entry['project'] = project

    return _entry


async def get_completed_batches_hail_api(
    token: str,
    last_completed_timestamp: Any | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """
    Get list of completed batches for the calling user,
    no filtering on billing_projects is done
    (optional): last_completed_timestamp (found in body of previous request)
    """

    params = {}

    if last_completed_timestamp:
        params['last_completed_timestamp'] = last_completed_timestamp

    for lim in (limit, 30, 15, 5):
        try:
            if lim:
                logger.info(f'Using limit {lim} to get batches')
                params['limit'] = lim
            q = '?' + '&'.join(f'{k}={v}' for k, v in params.items())
            url = HAIL_BATCHES_API + q
            logger.info(f'Getting batches: {url}')
            return await async_retry_transient_get_json_request(
                url,
                aiohttp.ClientError,
                headers={'Authorization': 'Bearer ' + token},
            )
        except asyncio.TimeoutError as ex:
            e = ex

    raise e


async def get_finished_batches_for_date(
    start: datetime,
    end: datetime,
    token: str,
    billing_project: str | None = None,
) -> list[BatchType]:
    """
    Get all the batches that started on {date} and are complete.
    We assume that batches are ordered by start time, so we can stop
    when we find a batch that started before the date.
    """
    batches: list[dict] = []
    last_completed_timestamp = math.ceil(end.timestamp() * 1000)
    n_requests = 0
    skipped = 0

    logger.info(f'Getting batches for range: [{start}, {end}]')

    while True:
        n_requests += 1
        jresponse = await get_completed_batches_hail_api(
            last_completed_timestamp=last_completed_timestamp,
            token=token,
        )

        if (
            'last_completed_timestamp' in jresponse
            and jresponse['last_completed_timestamp'] == last_completed_timestamp
        ):
            raise ValueError(
                'Something weird is happening with last_completed_timestamp: '
                f'{last_completed_timestamp}',
            )
        if n_requests > 0 and n_requests % 100 == 0:
            min_time_completed = min(b['time_completed'] for b in batches)
            logger.info(
                f'At {n_requests} requests ({min_time_completed}) for getting completed batches',
            )
        last_completed_timestamp = jresponse.get('last_completed_timestamp')
        if not jresponse.get('batches'):
            logger.error(f'No batches found for range: [{start}, {end}]')
            return batches
        for b in jresponse['batches']:
            # batch not finished or not finished within the (start, end) range

            time_completed = parse_hail_time(b['time_completed'])
            in_date_range = start <= time_completed < end

            if billing_project and billing_project != b.get('billing_project'):
                continue

            if time_completed < start:
                logger.info(
                    f'{billing_project} :: Got {len(batches)} batches '
                    f'in {n_requests} requests, skipping {skipped}',
                )
                return batches
            if in_date_range:
                batches.append(b)
            else:
                skipped += 1


async def get_jobs_for_batch(
    batch_id: int,
    token: str,
    limit: int | None = None,
) -> AsyncGenerator[list[JobType], None]:
    """
    For a single batch, fill in the 'jobs' field.
    """
    last_job_id = None
    end = False
    iterations = 0

    async with aiohttp.ClientSession() as session:
        while not end:
            iterations += 1

            if iterations > 1 and iterations % 5 == 0:
                logger.info(f'On {iterations} iteration to load jobs for {batch_id}')

            q = f'?limit={limit}' if limit else '?limit=9999'
            if last_job_id:
                q += f'&last_job_id={last_job_id}'
            url = HAIL_JOBS_API.format(batch_id=batch_id) + q

            jresponse: dict[
                Literal['last_job_id', 'jobs'],
                Any,
            ] = await async_retry_transient_get_json_request(
                url,
                (aiohttp.ClientError, asyncio.TimeoutError),
                session=session,
                headers={'Authorization': 'Bearer ' + token},
            )
            # stop if jobs DEFAULT_MAX_JOBS_PER_BATCH jobs
            new_last_job_id = jresponse.get('last_job_id')
            if new_last_job_id is None or new_last_job_id >= limit:
                end = True
            elif last_job_id:
                assert new_last_job_id > last_job_id
            last_job_id = new_last_job_id

            yield jresponse['jobs']


async def process_entries_from_hail_in_chunks(
    start: datetime,
    end: datetime,
    service_id: str,
    func_get_finalised_entries_for_batch: Callable[
        [BatchType, list[JobType]],
        Generator[dict[str, Any], None, None],
    ],
    billing_project: Optional[str] = None,
    batch_group_chunk_size: int = 10,
    log_prefix: str = '',
    mode: str = 'prod',
    output_path: str | None = './',
    func_batches_preprocessor: (
        Callable[[list[dict]], Awaitable[list[dict]]] | None
    ) = None,
) -> int:
    """
    Process all the seqr entries from hail batch,
    and insert them into the aggregate table.

    Break them down by dataset, and then proportion the rest of the costs.
    """

    # Get the existing ids from the table for optimisation,
    # avoiding multiple BQ calls
    existing_ids = retrieve_stored_ids(
        start,
        end,
        service_id,
        table=GCP_AGGREGATE_DEST_TABLE,
    )

    def insert_entries(_entries: list[dict[str, Any]]) -> int:
        if not _entries:
            return 0

        if mode in ('prod', 'dry-run'):
            return upsert_rows_into_bigquery(
                table=GCP_AGGREGATE_DEST_TABLE,
                objs=_entries,
                existing_ids=existing_ids,
                dry_run=mode == 'dry-run',
            )

        if mode == 'local':
            counter = 1
            if not output_path:
                raise ValueError('output_path must be provided in local mode')

            filename = os.path.join(output_path, f'processed-hail-{counter}.json')
            while os.path.exists(filename):
                counter += 1
                filename = os.path.join(output_path, f'processed-hail-{counter}.json')
            with open(filename, 'w+', encoding='utf-8') as file:
                logger.info(f'Writing {len(_entries)} to {filename}')
                # needs to be JSONL (line delimited JSON)
                file.writelines(rapidjson.dumps(e) + '\n' for e in _entries)

            return len(_entries)

        raise ValueError(f'Invalid mode: {mode}')

    # pylint: disable=too-many-locals
    token = get_hail_token()
    result = 0

    batches = await get_finished_batches_for_date(
        start=start,
        end=end,
        token=token,
        billing_project=billing_project,
    )

    if func_batches_preprocessor:
        batches = await func_batches_preprocessor(batches)
    if len(batches) == 0:
        return 0

    async def _get_jobs_and_add_to_queue(
        batch: BatchType,
        token: str,
        queue: asyncio.Queue[bool | tuple[BatchType, list[JobType]]],
    ) -> None:
        """
        Simpler wrapper to get jobs and adds to queue
        """
        batch_id = batch['id']
        jobs_cnt = batch['n_jobs']

        # only get the first jb if there are more than DEFAULT_MAX_JOBS_PER_BATCH jobs
        limit = 1 if jobs_cnt > DEFAULT_MAX_JOBS_PER_BATCH else None
        async for jobs in get_jobs_for_batch(batch_id, token, limit):
            if jobs_cnt <= DEFAULT_MAX_JOBS_PER_BATCH:
                await queue.put((batch, jobs))
            else:
                # This is most likely Hail Batch Query job, aggregate it as one job
                # batch contains all the costs as cost_breakdown
                # we just need to reformat it to match the JobType
                cost_breakdown = batch.get('cost_breakdown', [])

                total_jobs_cost = {}
                for rec in cost_breakdown:
                    total_jobs_cost[rec['resource']] = rec['cost']

                # construct total job record:
                total_job = {
                    'batch_id': batch_id,
                    'job_id': batch.get('n_jobs'),
                    'state': jobs[0].get('state'),  # pick from 1st jobs
                    'user': jobs[0].get('user'),  # pick from 1st jobs
                    'resources': {},  # not used
                    'cost': total_jobs_cost,
                    'attributes': {
                        'name': 'ALL JOBS COMBINED',
                    },
                }
                await queue.put((batch, [total_job]))

    async def _aggregate_and_insert(
        queue: asyncio.Queue[bool | tuple[BatchType, list[JobType]]],
    ) -> int:
        """
        Pull jobs from queue, transform using the get_finalised_entries_for_batch
        and then insert into bigquery, being careful to not load too many entries
        """

        entries: list[dict] = []
        result = 0
        while True:
            queue_item = await queue.get()
            if queue_item is True or queue_item is False:
                # this is the signal to stop
                break

            (batch, jobs) = queue_item

            if not jobs:
                continue

            for entry in func_get_finalised_entries_for_batch(batch, jobs):
                entries.append(entry)

                # insert at the DEFAULT_BQ_INSERT chunk size
                # this has a _small_ risk that there are some entries that are
                # HUGE, and we might go over the 10MB limit, but it's a small risk
                # given 2000 rows ~ 0.015 MB
                if len(entries) >= DEFAULT_BQ_INSERT_CHUNK_SIZE:
                    result += insert_entries(entries)
                    entries.clear()

        result += insert_entries(entries)
        return result

    # Process chunks of batches to avoid loading too many entries into memory
    nchnks = math.ceil(len(batches) / batch_group_chunk_size)
    lp = f'{log_prefix} ::' if log_prefix else ''

    for chunk_counter, batch_group in enumerate(chunk(batches, batch_group_chunk_size)):
        # we're going to fire off all the requests for jobs at once, and then:
        #   - use a task.Queue to synchronise the processing of the results
        #   - insert early if we're at 10MB across all the batches we're processing
        #        (rather than getting all jobs, which could be a lot of data)
        queue: asyncio.Queue[bool | tuple[BatchType, list[JobType]]] = asyncio.Queue()

        times = [b['time_created'] for b in batch_group]
        min_batch = min(times)
        max_batch = max(times)

        logger.info(
            f'{lp}Processing {len(batch_group)} batches in chunk '
            f'{chunk_counter}/{nchnks} [{min_batch}, {max_batch}]',
        )

        # kick off all the "gets" of the jobs. Note that each "get_jobs" happens
        # in multiple HTTP requests, so each _get_jobs_and_add_to_queue will update
        # the queue for each "n" jobs it gets, as it gets them
        tasks = [_get_jobs_and_add_to_queue(b, token, queue) for b in batch_group]

        # kick off the aggregator task
        aggregator_task = asyncio.create_task(_aggregate_and_insert(queue))
        await asyncio.gather(*tasks)

        # signal the aggregator task to stop
        await queue.put(True)

        # the aggregator reports the rows inserted
        result += await aggregator_task

    return result


RE_matcher = re.compile(r'-\d+$')


def billing_row_to_topic(row: dict[str, Any], dataset_to_gcp_map: dict) -> str | None:
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

    return RE_matcher.sub('', topic)


def upsert_rows_into_bigquery(
    objs: list[dict[str, Any]],
    existing_ids: set[str],
    dry_run: bool,
    table: str = GCP_AGGREGATE_DEST_TABLE,
    chunk_size: int = DEFAULT_BQ_INSERT_CHUNK_SIZE,
    max_chunk_size_mb: int = 6,
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
            f'adjusting the chunk size to {chunk_size}',
        )

    logger.debug(
        f'May insert {len(objs)} rows ({total_size_mb:.4f}MB) in {n_chunks} chunks',
    )

    inserts = 0
    inserted_ids: set[int] = set()

    for chunk_idx, chunked_objs in enumerate(chunk(objs, chunk_size)):
        # NOTE: it's possible to have valid duplicate rows
        # allow for adding duplicates on first upload only
        # Protects us against duplicate ids falling across chunks
        ids = {o['id'] for o in chunked_objs} - inserted_ids

        # Filter out any rows that are already in the table
        filtered_obj = [o for o in chunked_objs if o['id'] not in existing_ids]

        nrows = len(filtered_obj)

        if nrows == 0:
            logger.debug(
                f'Not inserting any rows 0/{len(chunked_objs)} '
                f'({chunk_idx+1}/{n_chunks} chunk)',
            )
            continue

        if dry_run:
            logger.info(
                f'DRY_RUN: Inserting {nrows}/{len(chunked_objs)} rows '
                f'({chunk_idx+1}/{n_chunks} chunk)',
            )
            inserts += nrows
            continue

        # Insert the new rows
        job_config = bq.LoadJobConfig()
        job_config.source_format = bq.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = get_formatted_bq_schema()

        j = '\n'.join(json.dumps(o) for o in filtered_obj)

        resp = get_bigquery_client().load_table_from_file(
            StringIO(j),
            table,
            job_config=job_config,
            project=GCP_PROJECT,
        )
        try:
            _result = resp.result()
        except ClientError as e:
            logger.error(resp.errors)
            raise e

        inserts += nrows
        inserted_ids = inserted_ids.union(ids)

    _is_s = '' if n_chunks == 1 else 's'
    logger.info(f'Inserted {inserts} rows in {n_chunks} chunk{_is_s}')
    return inserts


def upsert_aggregated_dataframe_into_bigquery(
    dataframe: pd.DataFrame,
    window_start: datetime,
    window_end: datetime,
    table: str = GCP_AGGREGATE_DEST_TABLE,
):
    """
    Upsert rows from a dataframe into the BQ.aggregate table.
    It must respect the schema defined in get_bq_schema_json().
    """

    if len(dataframe['id']) == 0:
        logger.info('No rows to insert')
        return 0

    # Cannot use query parameters for table names
    # https://cloud.google.com/bigquery/docs/parameterized-queries
    if '`' in table:
        raise ValueError(f'Table name ({table}) cannot contain backticks')
    _query = f"""
        SELECT id FROM {table}
        WHERE id IN UNNEST(@ids)
        AND DATE_TRUNC(usage_end_time, DAY) BETWEEN @window_start AND @window_end;
    """  # noqa: S608
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ArrayQueryParameter('ids', 'STRING', list(set(dataframe['id']))),
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

    result = get_bigquery_client().query(_query, job_config=job_config).result()
    existing_ids = set(result.to_dataframe()['id'])

    # Filter out any rows that are already in the table
    dataframe = dataframe[~dataframe['id'].isin(existing_ids)]

    # Count number of rows adding
    adding_rows = len(dataframe)

    # Insert the new rows
    project_id = table.split('.')[0]

    table_schema = get_bq_schema_json()
    dataframe.to_gbq(
        table,
        project_id=project_id,
        table_schema=table_schema,
        if_exists='append',
        chunksize=DEFAULT_BQ_INSERT_CHUNK_SIZE,
    )

    logger.info(f'{adding_rows} new rows inserted')
    return adding_rows


CACHED_CURRENCY_CONVERSION: dict[str, float] = {}


def get_currency_conversion_rate_for_time(time: datetime) -> float:
    """
    Get the currency conversion rate for a given time.
    Noting that GCP conversion rates are decided at the start of the month,
    and apply to each job that starts within the month, regardless of when
    the job finishes.
    """

    assert GCP_BILLING_BQ_TABLE

    window_start, window_end = get_invoice_month_range(time)
    # mfranklin: don't jump ahead of the start of the new invoice.month,
    #   it's only about 18 hours, but we'll use 22 to give some time for new billing
    #   data to be available.
    adjusted_time = time - timedelta(hours=22)
    window_start = window_start - timedelta(hours=22)
    key = f'{adjusted_time.year}{str(adjusted_time.month).zfill(2)}'
    if key not in CACHED_CURRENCY_CONVERSION:
        logger.info(f'Looking up currency conversion rate for {key}')
        if '`' in GCP_BILLING_BQ_TABLE:
            raise ValueError(
                f'Table name ({GCP_BILLING_BQ_TABLE}) cannot contain backticks',
            )
        query = f"""
            SELECT currency_conversion_rate
            FROM `{GCP_BILLING_BQ_TABLE}`
            WHERE invoice.month = @invoice_month
            AND DATE_TRUNC(usage_end_time, DAY) BETWEEN @window_start AND @window_end
            LIMIT 1
        """  # noqa: S608
        job_config = bq.QueryJobConfig(
            query_parameters=[
                bq.ScalarQueryParameter('invoice_month', 'STRING', key),
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
        query_result = (
            get_bigquery_client().query(query, job_config=job_config).result()
        )

        if query_result.total_rows == 0:
            logging.warning(f'Could not find billing data for {key!r}, for {time}')
            # find it from 2 days ago
            return get_currency_conversion_rate_for_time(time - timedelta(days=2))

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
    request: Request,
) -> tuple[datetime | None, datetime | None]:
    """
    Get the start and end times from the cloud function request.
    """
    if not request:
        return None, None

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
        return None, None

    if message := request_data.get('message'):
        if attributes := message.get('attributes'):
            if 'start' in attributes or 'end' in attributes:
                request_data = attributes
        elif 'data' in message:
            try:
                request_data = json.loads(b64decode(message['data']))
            except Exception as exp:
                raise exp

    logger.info(request_data)

    if not request_data or ('start' not in request_data and 'end' not in request_data):
        logger.warning('Could not find start or end. Defaulting to None.')
        raise ValueError("JSON is invalid, or missing a 'start' or 'end' property")

    try:
        start = request_data.get('start')
        end = request_data.get('end')
        start = datetime.fromisoformat(start) if start else start
        end = datetime.fromisoformat(end) if end else end
    except ValueError as err:
        logger.error(err)
        logger.error(f'Could not convert {start} or {end} to datetime')
        return None, None

    return start, end


def date_range_iterator(
    start: datetime,
    end: datetime,
    intv: timedelta = DEFAULT_RANGE_INTERVAL,
) -> Iterator[tuple[datetime, datetime]]:
    """
    Iterate over a range of dates.

    >>> list(date_range_iterator(datetime(2019, 1, 1), datetime(2019, 1, 2), intv=timedelta(days=2)))
    [(datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2019, 1, 2, 0, 0))]

    >>> list(date_range_iterator(datetime(2019, 1, 1), datetime(2019, 1, 3), intv=timedelta(days=2)))
    [(datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2019, 1, 3, 0, 0))]

    >>> list(date_range_iterator(datetime(2019, 1, 1), datetime(2019, 1, 4), intv=timedelta(days=2)))
    [(datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2019, 1, 3, 0, 0)), (datetime.datetime(2019, 1, 3, 0, 0), datetime.datetime(2019, 1, 4, 0, 0))]

    """
    dt_from = start
    dt_to = start + intv
    while dt_to < end:
        yield dt_from, dt_to
        dt_from += intv
        dt_to += intv

    dt_to = min(dt_to, end)
    if dt_from < dt_to:
        yield dt_from, dt_to


def get_start_and_end_from_data(
    data: str | dict | None,
) -> tuple[datetime | None, datetime | None]:
    """
    Get the start and end times from the cloud function data.
    """
    if data is not None:
        # Convert str to json
        if isinstance(data, str):
            try:
                data = dict(json.loads(data))
            except ValueError:
                return None, None

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

        return start, end

    return None, None


def process_default_start_and_end(
    start: datetime | None,
    end: datetime | None,
    interval: timedelta = DEFAULT_RANGE_INTERVAL,
) -> tuple[datetime, datetime]:
    """
    Take input start / end values, and apply
    defaults
    """
    _end = end.astimezone(timezone.utc) if end else datetime.now(tz=timezone.utc)
    _start = start.astimezone(timezone.utc) if start else _end - interval

    assert isinstance(_start, datetime) and isinstance(_end, datetime)
    return _start, _end


def get_date_intervals_for(
    start: datetime | None,
    end: datetime | None,
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
    labels: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """
    Get well-formed entry dictionary from keys
    """

    assert labels is None or isinstance(labels, dict)

    _labels = None
    if labels:
        # convert to string
        _labels = rapidjson.dumps(labels, sort_keys=True)
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
        'system_labels': None,
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


def infer_batch_namespace(batch: dict) -> str:
    """
    Infer the namespace from the batch attributes
    """
    namespace = batch.get('attributes', {}).get('namespace')
    user = batch.get('user')
    default = None
    if namespace:
        return namespace

    if user:
        if 'test' in user:
            return 'test'
        if 'standard' in user:
            return 'main'
        if 'full' in user:
            return 'main'

    return default


def reformat_bigqquery_labels(data: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Convert from {'key': 'KEY1', 'value': 'VAL1'} to {'KEY1': 'VAL1'}
    and keep other keys as there are
    """
    labels = {}
    for kv in data:
        if 'key' in kv:
            labels[kv['key']] = kv['value']
        else:
            # otherwise keep the original key
            for k, v in kv.items():
                labels[k] = v

    return labels


def retrieve_stored_ids(
    start: datetime,
    end: datetime,
    service_id: str,
    table: str = GCP_AGGREGATE_DEST_TABLE,
) -> set[str]:
    """
    Retrieve all the stored ids using seqr- and hail- prefixes
    """
    logger.info(
        f'Retrieving stored ids for {table} between {start} and {end}',
    )

    if '`' in table:
        raise ValueError('Table name cannot contain backticks')

    if service_id not in ('seqr', 'hail'):
        raise ValueError(f'Invalid service_id: {service_id}')

    if table not in (GCP_AGGREGATE_DEST_TABLE, GCP_BILLING_BQ_TABLE):
        raise ValueError(f'Invalid table: {table}')

    _query = f"""
        SELECT id FROM `{table}`
        WHERE DATE_TRUNC(usage_end_time, DAY) BETWEEN @window_start AND @window_end
        AND id LIKE '{service_id}-%';
    """  # noqa: S608 both tables and service_id are checked for validity

    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter(
                'window_start',
                'STRING',
                start.strftime('%Y-%m-%d'),
            ),
            bq.ScalarQueryParameter(
                'window_end',
                'STRING',
                end.strftime('%Y-%m-%d'),
            ),
        ],
    )

    records = set()
    result = get_bigquery_client().query(_query, job_config=job_config).result()
    records = set(result.to_dataframe()['id'])
    logger.info(
        f'Retrieved {len(records)} stored ids',
    )

    return records
