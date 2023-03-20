#!/usr/bin/env python3

"""Computes aggregate bucket disk usage stats."""

from collections import defaultdict
import gzip
import json
import logging
from cloudpathlib import AnyPath
from cpg_utils.hail_batch import get_config, output_path
from google.cloud import storage


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
    if (slash_index := name.rfind('/')) != -1:
        return name[:slash_index]
    return ''  # Root level


def main():
    """Main entrypoint."""
    # Don't print DEBUG logs from urllib3.connectionpool.
    logging.getLogger().setLevel(logging.INFO)

    storage_client = storage.Client()
    dataset = get_config()['workflow']['dataset']
    access_level = get_config()['workflow']['access_level']

    aggregate_stats = defaultdict(lambda: defaultdict(int))
    for bucket_suffix in BUCKET_SUFFIXES:
        if access_level == 'test' and not bucket_suffix.startswith('test'):
            continue  # Skip main buckets when testing.

        bucket_name = f'cpg-{dataset}-{bucket_suffix}'
        logging.info(f'Listing blobs in {bucket_name}...')
        blobs = storage_client.list_blobs(bucket_name)
        count = 0
        for blob in blobs:
            count += 1
            if count % 10**6 == 0:
                logging.info(f'{count // 10**6} M blobs...')
            folder = f'/{aggregate_level(blob.name)}'
            while True:
                path = f'gs://{bucket_name}{folder}'
                stats = aggregate_stats[path]
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

    output = output_path('disk_usage.json.gz', 'analysis')
    logging.info(f'Writing results to {output}...')
    with AnyPath(output).open('wb') as f:
        with gzip.open(f, 'wt') as gzf:
            json.dump(aggregate_stats, gzf)


if __name__ == '__main__':
    main()
