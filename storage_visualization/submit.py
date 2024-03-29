#!/usr/bin/env python3

"""Submits `main.py` to the analysis-runner, with a full set of dataset names based on a yaml config."""

import datetime
import os
import sys

import yaml

# See requirements.txt for why we're disabling the linter warnings here.
from analysis_runner.cli_analysisrunner import (
    run_analysis_runner,  # pylint: disable=import-error
)

ALL_DATASETS = 'all-datasets'


def main():
    """Main entrypoint."""
    if len(sys.argv) != 2:  # noqa: PLR2004
        print('Usage: submit.py <config.yaml>')
        sys.exit(1)

    with open(sys.argv[1], encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # Generate a list of all datasets to invoke main.py with.
    datasets = sorted(set(config.keys()) - {ALL_DATASETS})

    run_analysis_runner(
        dataset=ALL_DATASETS,
        access_level=os.getenv('ACCESS_LEVEL', 'full'),
        config=['storage_visualization/slack.toml'],
        output_dir='storage_visualization/'
        + datetime.datetime.now().date().strftime('%y-%m-%d'),  # noqa: DTZ005
        description='Storage visualization',
        script=['storage_visualization/main.py', *datasets],
    )


if __name__ == '__main__':
    main()
