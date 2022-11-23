#!/usr/bin/env python3

"""Updates all stacks in an order compatible with each stack dependency."""

from collections import defaultdict

import os
import glob
import graphlib  # TopologicalSorter requires python >= 3.9.
import subprocess
import yaml

from stack_utils import get_pulumi_config_passphrase  # pylint: disable=import-error

COMMON_DATASET = 'common'

deps = defaultdict(list)
for filename in glob.glob('Pulumi.*.yaml'):
    with open(filename, encoding='utf-8') as f:
        parsed = yaml.safe_load(f)
    dataset = filename.split('.')[1]
    if depends_on := parsed['config'].get('datasets:depends_on'):
        # Parse the string representation of the list.
        deps[dataset] = yaml.safe_load(depends_on)

    deps[dataset] += [COMMON_DATASET]

deps['common'] = []

env = dict(
    os.environ,
    PULUMI_CONFIG_PASSPHRASE=get_pulumi_config_passphrase(),
    CPG_CONFIG_PATH=os.path.abspath('cpg.toml'),
)

for dataset in graphlib.TopologicalSorter(deps).static_order():
    print(f'Updating {dataset}...')
    subprocess.check_call(['pulumi', 'up', '--stack', dataset, '-y'], env=env)
