import os
import glob
import json
import subprocess

from stack.stack_utils import get_pulumi_config_passphrase

resources = []
if os.path.basename(os.getcwd()) != 'stack':
    raise ValueError('Please run this script in the "stack" subdirectory"')

types_to_ignore = {
    'gcp:projects/service:Service',
}

datasets_to_ignore = {'production', 'reference'}


def process_urn(dataset: str, urn: str) -> str | None:
    # "urn:pulumi:acute-care::datasets::gcp:cloudidentity/groupMembership:GroupMembership::hail-service-account-standard-cromwell-access"
    fields = urn.split("::")
    if len(fields) != 4:
        raise ValueError(f'Cannot detect name from URN: {urn}')

    key_name = fields[-1]

    if '::gcp' in urn:
        if key_name.startswith('gcp'):
            if key_name.startswith(f'gcp-{dataset}'):
                return key_name
            else:
                return '-'.join(['gcp', dataset, *key_name.split('-')[1:]])
        if key_name.startswith(dataset):
            return f'gcp-' + dataset
        return f'gcp-{dataset}-{key_name}'

    if '::azure' in urn:
        if key_name.startswith('az'):
            if key_name.startswith(f'az-{dataset}'):
                return 'azure' + key_name[2:]
            else:
                return '-'.join(['azure', dataset, *key_name.split('-')[1:]])
        if key_name.startswith(dataset):
            return f'azure-' + dataset
        return f'azure-{dataset}-{key_name}'

    return key_name

for filename in glob.glob('Pulumi.*.yaml'):

    dataset = filename.split('.')[1]

    env = dict(
        os.environ,
        PULUMI_CONFIG_PASSPHRASE=get_pulumi_config_passphrase(),
        CPG_CONFIG_PATH=os.path.abspath('cpg.toml'),
    )

    state_filename = f'pulumi-{dataset}-state.json'
    if not os.path.exists(state_filename):
        subprocess.check_output(
            ['pulumi', 'stack', 'export', '-s', dataset, '--file', state_filename],
            env=env,
        )

    with open(state_filename) as f:
        dataset_dict = json.load(f)

    for resource in dataset_dict['deployment']['resources']:

        if not resource.get('id'):
            print(f'Bad resource: {resource["urn"]}')
            continue

        resources.append({
            "name": process_urn(dataset, resource['urn']),
            "type": resource['type'],
            "id": resource['id'],
        })


with open('pulumi-production-state.json', 'w+') as f:
    json.dump({"resources": resources}, f, indent=2)
