#!/usr/bin/env python3

"""Produces a treemap visualization from disk usage summary stats."""

import argparse
import gzip
import json
import logging
import os
import re
from collections import defaultdict
from typing import Any

import humanize
import pandas as pd
import plotly.express as px
from cloudpathlib import AnyPath

from cpg_utils import to_path
from cpg_utils.config import (
    config_retrieve,
    dataset_path,
    get_access_level,
    output_path,
)
from cpg_utils.slack import upload_file

ROOT_NODE = '<root>'
DATASET_REGEX = re.compile(r'gs:\/\/cpg-([A-z0-9-]+)-(main|test)')
DOCKER_IMAGE = (
    'australia-southeast1-docker.pkg.dev/cpg-common/images/storage-visualization:latest'
)

BATCH_ID = os.getenv('HAIL_BATCH_ID')
JOB_ID = os.getenv('HAIL_JOB_ID')


def _get_hail_batch_url(
    batch_id: str | None = BATCH_ID,
    job_id: str | None = JOB_ID,
) -> str | None:
    if not batch_id:
        return None
    base = f'https://batch.hail.populationgenomics.org.au/batches/{batch_id}'
    if job_id is None:
        return base

    return f'{base}/jobs/{job_id}'


def get_parser():
    """
    Get command line for this script
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        help='The path to the gzipped input JSON; supports cloud paths and can be specified multiple times',
        required=True,
        action='append',
    )
    parser.add_argument(
        '--max-depth',
        help='Maximum folder depth to display',
        default=3,
        type=int,
    )
    parser.add_argument(
        '--group-by-dataset',
        help='Group buckets by the dataset',
        action='store_true',
    )
    parser.add_argument(
        '--post-slack-message',
        help='Post the generated treemap to Slack',
        action='store_true',
    )
    parser.add_argument(
        '--bucket-type',
        help='Optional bucket type to scan (upload, tmp, analysis, web)',
    )

    return parser


def form_row(name: str, parent: str, values: dict[str, Any]) -> tuple:
    size = values['size']
    standard_bytes = values.get('STANDARD_bytes', 0)
    nearline_bytes = values.get('NEARLINE_bytes', 0)
    coldline_bytes = values.get('COLDLINE_bytes', 0)
    archive_bytes = values.get('ARCHIVE_bytes', 0)
    # Map total bytes to a single `hotness` value in 0..1 based on storage class.
    hotness = (
        standard_bytes * 1
        + nearline_bytes * 2 / 3
        + coldline_bytes * 1 / 3
        + archive_bytes * 0
    ) / size
    return (
        name,
        parent,
        values['monthly_storage_cost'],
        humanize.naturalsize(size, binary=True),
        humanize.naturalsize(standard_bytes, binary=True),
        humanize.naturalsize(nearline_bytes, binary=True),
        humanize.naturalsize(coldline_bytes, binary=True),
        humanize.naturalsize(archive_bytes, binary=True),
        humanize.intcomma(values['num_blobs']),
        f'{values["monthly_storage_cost"]:.2f} USD',
        hotness,
    )


def post_to_slack(
    treemap_png_path: str,
    treemap_html_web_url: str,
    missing_datasets: list[str],
):
    """Posts the URL of the generated treemap together with a preview image to Slack."""
    with AnyPath(treemap_png_path).open('rb') as f:
        content = f.read()

    comment = 'Storage visualization: ' + treemap_html_web_url
    if missing_datasets:
        comment += '\nMissing datasets: ' + ', '.join(str(e) for e in missing_datasets)

        if url := _get_hail_batch_url(job_id=None):
            comment += f'\n\nSee {url} for more details.'

    upload_file(
        content=content,
        comment=comment,
        title='Visualization preview',
    )


def prepare_chart(df: pd.DataFrame):
    fig = px.treemap(
        df,
        names='name',
        parents='parent',
        values='value',
        color='hotness',
        hover_name='name',
        hover_data={
            'name': False,
            'parent': False,
            'value': False,
            'total_bytes': True,
            'standard_bytes': True,
            'nearline_bytes': True,
            'coldline_bytes': True,
            'archive_bytes': True,
            'num_blobs': True,
            'monthly_storage_cost': True,
            'hotness': False,
        },
        color_continuous_scale='Bluered',
        range_color=(0, 1),
    )
    fig.update_traces(root_color='lightgrey')
    return fig


def generate_and_write_treemap(
    rows: list[tuple],
    max_depth: int,
    output_html: str,
    output_png: str,
):
    dataframe = pd.DataFrame(
        # The column name list needs to match the `append_row` implementation.
        rows,
        columns=(
            'name',
            'parent',
            'value',  # Gets mapped to treemap node size.
            'total_bytes',
            'standard_bytes',
            'nearline_bytes',
            'coldline_bytes',
            'archive_bytes',
            'num_blobs',
            'monthly_storage_cost',
            'hotness',  # Gets mapped to treemap node color.
        ),
    )

    fig = prepare_chart(dataframe)
    with AnyPath(output_html).open('wt') as f:
        fig.write_html(f)

    # for image show only top (max_depth - 1) levels
    # name starts with gs:// so add 2)
    max_slash_count = 2 + (max_depth - 1)
    dataframe = dataframe[dataframe['name'].str.count('/') < max_slash_count]
    fig = prepare_chart(dataframe)
    with AnyPath(output_png).open('wb') as f:
        fig.write_image(f, width=1920, height=1080)


def prepare_rows_from_input_paths(  # noqa: C901
    input_paths: list[str],
    max_depth: int,
    group_by_dataset: bool,
    bucket_type: str | None,
) -> tuple[list[tuple], list[str]]:
    """Prepare rows for a dataframe from the given input paths.

    Args:
        input_paths (list[str]): json.gz files produced from disk_usage.py
        max_depth (int): Maximum folder depth to display
        group_by_dataset (bool): Group bucket storage stats by their dataset
        bucket_type (str | None): Optional bucket type to subset to (upload, tmp, analysis, web)

    Returns:
        tuple[list[tuple], list[str]]: (rows, errors)
    """
    rows: list[tuple] = []

    access_level = 'test' if get_access_level() == 'test' else 'main'
    root_values: dict[str, int] = defaultdict(int)
    datasets: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    missing_datasets: list[str] = []
    for input_path in input_paths:
        logging.info(f'Processing {input_path}')
        p = AnyPath(input_path)

        if not p.exists():
            dataset = os.path.basename(input_path).removesuffix('.json.gz')
            missing_datasets.append(dataset)
            continue

        with AnyPath(input_path).open('rb') as f, gzip.open(f, 'rt') as gfz:
            for name, vals in json.load(gfz).items():
                # If a bucket type filter is set, skip non-matching buckets.
                if bucket_type is not None and not to_path(name).bucket.endswith(
                    f'-{access_level}-{bucket_type}'
                ):
                    continue
                depth = name.count('/') - 1  # Don't account for gs:// scheme.
                if depth > max_depth:
                    continue
                size = vals['size']
                if not size:
                    continue
                # Strip one folder for the parent name. Map `gs://` to the
                # predefined treemap root node label.
                slash_index = name.rfind('/')
                if slash_index > len('gs://'):
                    parent = name[:slash_index]
                else:
                    for k, v in vals.items():
                        root_values[k] += v
                    match = DATASET_REGEX.search(name)
                    # fall back to ROOT_NODE if can't determine parent
                    dataset = match.groups()[0] if match else None
                    if dataset and group_by_dataset:
                        parent = dataset
                        for k, v in vals.items():
                            datasets[dataset][k] += v
                    else:
                        parent = ROOT_NODE

                rows.append(form_row(name, parent, vals))

    for dataset, values in datasets.items():
        rows.append(form_row(dataset, ROOT_NODE, values))

    # Finally, add the overall root.
    rows.append(form_row(ROOT_NODE, '', root_values))

    return rows, missing_datasets


def main() -> None:
    """Main entrypoint."""

    logging.getLogger().setLevel(logging.INFO)
    args = get_parser().parse_args()

    should_post_to_slack = args.post_slack_message

    try:
        rows, missing_datasets = prepare_rows_from_input_paths(
            args.input,
            args.max_depth,
            args.group_by_dataset,
            args.bucket_type,
        )

        logging.info('Writing results')
        # HTML output path with a datestamp
        output_file_name = (
            f'{args.bucket_type}_treemap.html' if args.bucket_type else 'treemap.html'
        )
        output_html_path = output_path(
            output_file_name, category='web', dataset='common'
        )
        # HTML output path to a fixed location for the most recent treemap
        output_prefix = config_retrieve(['workflow', 'output_prefix']).split('/')[0]
        fixed_html_path = dataset_path(
            f'{output_prefix}/{output_file_name}', category='web', dataset='common'
        )
        fixed_web_html_path = dataset_path(
            f'{output_prefix}/{output_file_name}',
            category='web_url',
            dataset='common',
        )

        web_html_path = output_path(
            output_file_name,
            category='web_url',
            dataset='common',
        )
        generate_and_write_treemap(
            rows=rows,
            max_depth=args.max_depth,
            output_html=output_html_path,
            # write locally to use in slack message
            output_png='treemap.png',
        )

        if config_retrieve(['workflow', 'use_fixed_url'], default=False):
            # copy to fixed location, overwriting previous
            AnyPath(fixed_html_path).write_bytes(AnyPath(output_html_path).read_bytes())
            web_html_path = fixed_web_html_path

        if should_post_to_slack:
            post_to_slack(
                treemap_png_path='treemap.png',
                treemap_html_web_url=web_html_path,
                missing_datasets=missing_datasets,
            )
    except (ValueError, TypeError) as error:
        # limit the comment message to prevent overloading slack with large error dumps.
        # can always check the errors in the Hail output.
        comment = f'Failed to generate storage viz treemap: {error}'[:250]
        if url := _get_hail_batch_url():
            comment += f'\n\nSee {url} for more details.'
        if should_post_to_slack:
            upload_file(
                content=b'Failed to generate storage viz treemap',
                comment=comment,
            )
        raise Exception(comment) from error


if __name__ == '__main__':
    main()
