#!/usr/bin/env python3

"""Submits `main.py` to the analysis-runner, with a full set of RD dataset names based on a yaml config."""

import datetime
import os
import sys

import yaml

# See requirements.txt for why we're disabling the linter warnings here.
from analysis_runner.cli_analysisrunner import (
    run_analysis_runner,  # pylint: disable=import-error
)

RD_DATASET = 'rare-disease'


def main():
    """Main entrypoint."""
    if len(sys.argv) != 2 and len(sys.argv) != 3:  # noqa: PLR2004
        print('Usage: submit.py <config.yaml> <optional: bucket_type>')
        sys.exit(1)

    with open(sys.argv[1], encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # If the upload/tmp/analysis/web flag is present, only scan those buckets.
    if sys.argv[2] in ['upload', 'tmp', 'analysis', 'web']:
        bucket_type = sys.argv[2]
    else:
        bucket_type = None

    # Generate a list of all RD datasets to invoke main.py with.
    datasets = sorted(set(config[RD_DATASET]['depends_on']))

    run_analysis_runner(
        dataset=RD_DATASET,
        access_level=os.getenv('ACCESS_LEVEL', 'full'),
        config=['storage_visualization/rd_slack.toml'],
        output_dir='rd_storage_visualization/'
        + datetime.datetime.now().date().strftime('%y-%m-%dT%H:%M:%S'),  # noqa: DTZ005
        description='RD projects storage visualization',
        script=['storage_visualization/main.py', *datasets, bucket_type]
        if bucket_type
        else ['storage_visualization/main.py', *datasets],
    )


if __name__ == '__main__':
    main()
