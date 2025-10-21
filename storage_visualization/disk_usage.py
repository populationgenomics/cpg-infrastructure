#!/usr/bin/env python3

"""Computes aggregate bucket disk usage stats."""

import gzip
import json
import logging
import sys
from collections import defaultdict

import google.api_core.exceptions
from cloudpathlib import AnyPath
from google.cloud import storage
from humanize import naturalsize

from cpg_utils.config import get_access_level

# It's important not to list the `archive` bucket here, as Class B operations are very
# expensive for that storage class.
BUCKET_SUFFIXES = [
    'main',
    'main-analysis',
    'main-tmp',
    'main-upload',
    'main-web',
    'test',
    'test-analysis',
    'test-tmp',
    'test-upload',
    'test-web',
]

STORAGE_COST_MONTHLY_PER_GB_SYDNEY = {
    'STANDARD': 0.023,
    'NEARLINE': 0.016,
    'COLDLINE': 0.006,
    'ARCHIVE': 0.0025,
}


def aggregate_level(name: str) -> str:
    """
    Returns a prefix for the given blob name at the folder or Hail table level.

    >>> aggregate_level('some/folder/and/a/filename.bam')
    'some/folder/and/a'
    >>> aggregate_level('some/folder/and/a/hail_table.ht/index/files/part12345')
    'some/folder/and/a/hail_table.ht'
    >>> aggregate_level('some/folder/and/a/hail_matrix_table.mt/index/files/part12345')
    'some/folder/and/a/hail_matrix_table.mt'
    >>> aggregate_level('file_in_root.cram')
    """
    if (ht_index := name.find('.ht/')) != -1:
        return name[: ht_index + 3]
    if (mt_index := name.find('.mt/')) != -1:
        return name[: mt_index + 3]
    if (vds_index := name.find('.vds/')) != -1:
        return name[: vds_index + 4]
    if name.startswith('batch-tmp'):
        return name[:9]
    if (batch_tmp_index := name.rfind('/batch-tmp')) != -1:
        return name[: batch_tmp_index + 10]
    if (slash_index := name.rfind('/')) != -1:
        return name[:slash_index]
    return ''  # Root level


def main():
    """Main entrypoint."""
    if len(sys.argv) != 3 and len(sys.argv) != 4:  # noqa: PLR2004
        print('Usage: disk_usage.py <dataset> <output.json.gz> <optional: bucket_type>')
        sys.exit(1)

    # Don't print DEBUG logs from urllib3.connectionpool.
    logging.getLogger().setLevel(logging.INFO)

    storage_client = storage.Client()
    dataset = sys.argv[1]

    # If the upload/tmp/analysis/web flag is present, only scan those buckets.
    if len(sys.argv) == 4:  # noqa: PLR2004
        bucket_type = sys.argv[3]
        bucket_suffixes = [
            suffix for suffix in BUCKET_SUFFIXES if suffix.endswith(bucket_type)
        ]
        if not bucket_suffixes:
            logging.error(f'No bucket suffixes found for bucket type "{bucket_type}".')
            sys.exit(1)
    else:
        bucket_suffixes = BUCKET_SUFFIXES

    access_level = get_access_level()

    aggregate_stats: defaultdict[str, defaultdict[str, int]] = defaultdict(
        lambda: defaultdict(int),
    )
    for bucket_suffix in bucket_suffixes:
        if access_level == 'test' and not bucket_suffix.startswith('test'):
            continue  # Skip main buckets when testing.

        bucket_name = f'cpg-{dataset}-{bucket_suffix}'
        count_stats_for_bucket(
            storage_client=storage_client,
            aggregate_stats_container=aggregate_stats,
            bucket_name=bucket_name,
        )

    output = sys.argv[2]
    logging.info(f'Writing results to {output}...')
    with AnyPath(output).open('wb') as f, gzip.open(f, 'wt') as gzf:
        json.dump(aggregate_stats, gzf)  # type: ignore[arg-type, PGH003]


def count_stats_for_bucket(
    storage_client: storage.Client,
    aggregate_stats_container: defaultdict,
    bucket_name: str,
):
    """
    Calculate blob statistics for the given bucket and
    put them in the aggregate_stats_container.
    """
    try:
        logging.info(f'Listing blobs in {bucket_name}...')
        blobs = storage_client.list_blobs(bucket_name)
        count = 0
        for blob in blobs:
            count += 1
            if count % 10**6 == 0:
                s = naturalsize(sys.getsizeof(aggregate_stats_container))
                logging.info(f'{count // 10**6} M blobs... aggregate dict is using {s}')
            folder = f'/{aggregate_level(blob.name)}'
            while True:
                path = f'gs://{bucket_name}{folder}'
                stats = aggregate_stats_container[path]
                stats['size'] += blob.size
                stats[f'{blob.storage_class}_bytes'] += blob.size
                stats['num_blobs'] += 1
                size_in_gb = blob.size / 2**30
                stats['monthly_storage_cost'] += (  # Assumes the bucket is in Sydney.
                    size_in_gb * STORAGE_COST_MONTHLY_PER_GB_SYDNEY[blob.storage_class]
                )
                if not folder:
                    break
                folder = folder[: folder.rfind('/')]

        logging.info(f'{bucket_name} contains {count} blobs.')

    except google.api_core.exceptions.NotFound:
        logging.warning(f'Bucket {bucket_name} not found.')
        return


if __name__ == '__main__':
    main()
