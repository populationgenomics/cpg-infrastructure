#!/usr/bin/env python3

"""Prints the Google Cloud project IDs for datasets that have not
been migrated to Autoclass yet."""

import sys
import yaml


def main():
    """Main entry point."""

    if len(sys.argv) != 2:
        print(f'syntax: {sys.argv[0]} <config.yaml>')
        sys.exit(1)

    with open(sys.argv[1], 'rt', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    for dataset in config.values():
        if not dataset.get('autoclass'):
            print(dataset['gcp']['project'])


if __name__ == '__main__':
    main()
