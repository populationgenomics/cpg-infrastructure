#!/usr/bin/env python3

"""Submits `main.py` to the analysis-runner, with a full set of dataset names based on a yaml config."""

import datetime
import os
import sys
import yaml

# See requirements.txt for why we're disabling the linter warnings here.
from analysis_runner.cli_analysisrunner import (  # pylint: disable=import-error
    run_analysis_runner,
)

ALL_DATASETS = 'all-datasets'


def main():
    """Main entrypoint."""
    if len(sys.argv) != 2:
        print('Usage: submit.py <config.yaml>')
        sys.exit(1)

    with open(sys.argv[1], encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # Generate a list of all datasets to invoke main.py with.
    datasets = sorted(list(set(config.keys()) - {ALL_DATASETS}))

    run_analysis_runner(
        dataset=ALL_DATASETS,
        access_level=os.getenv('ACCESS_LEVEL', 'standard'),
        config=['storage_visualization/slack.toml'],
        output_dir='storage_visualization/'
        + datetime.date.today().strftime('%y-%m-%d'),
        description='Storage visualization',
        script=['storage_visualization/main.py'] + datasets,
    )


if __name__ == '__main__':
    main()
