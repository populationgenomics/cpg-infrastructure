#!/usr/bin/env python3

"""Produces a treemap visualization from disk usage summary stats."""

import argparse
import gzip
import json
import logging
import re
from collections import defaultdict
import humanize
from cloudpathlib import AnyPath
import pandas as pd
import plotly.express as px

ROOT_NODE = '<root>'
DATASET_REGEX = re.compile(r'gs:\/\/cpg-([A-z0-9-]+)-(main|test)')


def main():
    """Main entrypoint."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        help='The path to the gzipped input JSON; supports cloud paths and can be specified multiple times',
        required=True,
        action='append',
    )
    parser.add_argument(
        '--output',
        help='The path to the output HTML report; supports cloud paths',
        required=True,
    )
    parser.add_argument(
        '--max-depth',
        help='Maximum folder depth to display',
        default=3,
        type=int,
    )
    parser.add_argument(
        '--group-by-dataset', help='Group buckets by the dataset', action='store_true'
    )
    args = parser.parse_args()

    logging.getLogger().setLevel(logging.INFO)

    rows = []

    def append_row(name, parent, values):
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
        rows.append(
            (
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
        )

    root_values = defaultdict(int)
    group_by_dataset = args.group_by_dataset
    datasets: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for input_path in args.input:
        logging.info(f'Processing {input_path}')
        with AnyPath(input_path).open('rb') as f:
            with gzip.open(f, 'rt') as gfz:
                for name, vals in json.load(gfz).items():
                    depth = name.count('/') - 1  # Don't account for gs:// scheme.
                    if depth > args.max_depth:
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

                    append_row(name, parent, vals)

    for dataset, values in datasets.items():
        append_row(dataset, ROOT_NODE, values)

    # Finally, add the overall root.
    append_row(ROOT_NODE, '', root_values)

    df = pd.DataFrame(
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

    logging.info(f'Writing result to {args.output}')
    fig.write_html(args.output)


if __name__ == '__main__':
    main()
