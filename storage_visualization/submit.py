#!/usr/bin/env python3

"""Submits `main.py` to the analysis-runner, with a full set of dataset names based on a yaml config."""

import datetime
import subprocess
import sys
import yaml


def main():
    if len(sys.argv) != 2:
        print('Usage: submit.py <config.yaml>')
        sys.exit(1)

    with open(sys.argv[1], encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # Generate a list of all datasets to invoke main.py with.
    ALL_DATASETS = 'all-datasets'
    datasets = sorted(list(set(config.keys()) - {ALL_DATASETS}))

    subprocess.check_output(
        [
            'analysis-runner',
            'submit',
            '--dataset',
            ALL_DATASETS,
            '--access-level',
            'full',
            '--output-dir',
            f'storage_visualization_{datetime.date.today().strftime("%y-%m-%d")}',
            '--description',
            'Storage visualization',
            'main.py',
        ]
        + datasets
    )


if __name__ == '__main__':
    main()
