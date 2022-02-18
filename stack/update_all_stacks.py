#!/usr/bin/env python

"""Updates all stacks in an order compatible with each stack dependency."""

import glob
import graphlib  # TopologicalSorter requires python >= 3.9.
import os
import subprocess
import yaml

deps = {}
for filename in glob.glob('Pulumi.*.yaml'):
    with open(filename, encoding='utf-8') as f:
        parsed = yaml.safe_load(f)
    dataset = filename.split('.')[1]
    deps[dataset] = parsed['config'].get('datasets:depends_on', [])
    if deps[dataset]:
        # Parse the string representation of the list.
        deps[dataset] = yaml.safe_load(deps[dataset])

env = dict(os.environ, PULUMI_CONFIG_PASSPHRASE='')
for dataset in graphlib.TopologicalSorter(deps).static_order():
    print(f'Updating {dataset}...')
    subprocess.check_call(['pulumi', 'stack', 'select', dataset], env=env)
    subprocess.check_call(['pulumi', 'up', '-y'], env=env)
