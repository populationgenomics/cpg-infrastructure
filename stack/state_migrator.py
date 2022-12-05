import os
import glob
import json
import subprocess

import yaml

from stack.stack_utils import get_pulumi_config_passphrase

resources = []
if os.path.basename(os.getcwd()) != 'stack':
    raise ValueError('Please run this script in the "stack" subdirectory"')

types_to_ignore = {
    'gcp:projects/service:Service',
}

datasets_to_ignore = {'production', 'reference'}

GCP_BUCKET_OBJECT_TYPE = "gcp:storage/bucketObject:BucketObject"
GCP_BUCKET_TYPE = "gcp:storage/bucket:Bucket"
GCP_BUCKET_MEMBERSHIP_TYPE = "gcp:storage/bucketIAMMember:BucketIAMMember"
GCP_SECRET_MEMBERSHIP_TYPE = "gcp:secretmanager/secretIamMember:SecretIamMember"
GCP_REPOSITORY_MEMBERSHIP_TYPE = "gcp:artifactregistry/repositoryIamMember:RepositoryIamMember"
GCP_GROUP_TYPE = 'gcp:cloudidentity/group:Group'
AZURE_BLOB_CONTAINER_TYPE = 'azure-native:storage:BlobContainer'
AZURE_GROUP_TYPE = 'azuread:index/group:Group'


membership_types = {
    "gcp:projects/iAMMember:IAMMember",
    GCP_REPOSITORY_MEMBERSHIP_TYPE,
    GCP_SECRET_MEMBERSHIP_TYPE,
    GCP_BUCKET_MEMBERSHIP_TYPE
}


def process_urn(dataset: str, urn: str) -> str | None:
    """

    >>> process_urn('common', 'urn:pulumi:common::datasets::azuread:index/group:Group::az-common-access')
    'common-azure-access'
    """
    # "urn:pulumi:acute-care::datasets::gcp:cloudidentity/groupMembership:GroupMembership::hail-service-account-standard-cromwell-access"

    forbidden_urns = [
        f'urn:pulumi:{dataset}::datasets::pulumi:pulumi:Stack::',
        f'urn:pulumi:{dataset}::datasets::pulumi:pulumi:StackReference::',
    ]

    if any(urn.startswith(fbdn) for fbdn in forbidden_urns):
        return None

    fields = urn.split("::")
    if len(fields) != 4:
        raise ValueError(f'Cannot detect name from URN: {urn}')

    if GCP_BUCKET_OBJECT_TYPE in urn:
        # can't migrate objects / credentials, they have to be recreated
        return None

    ftype = fields[-2]
    if ftype.startswith('pulumi:providers:'):
        return None

    key_name = fields[-1]

    if AZURE_GROUP_TYPE in urn:
        key_name += '-group'
    elif AZURE_BLOB_CONTAINER_TYPE in urn:
        key_name += '-blob-container'
    elif GCP_GROUP_TYPE in urn:
        key_name += '-group'
    elif GCP_BUCKET_TYPE in urn:
        if key_name.startswith(f'cpg-{dataset}'):
            bucket_type = key_name.removeprefix(f'cpg-{dataset}-')
            return '-'.join([dataset, 'gcp', bucket_type, 'bucket'])
        key_name += '-bucket'

    if '::gcp' in urn:
        if key_name.startswith(dataset):
            if key_name.startswith(f'{dataset}-gcp'):
                return key_name
            else:
                return '-'.join([dataset, 'gcp', *key_name.split('-')[1:]])
        if key_name.startswith('gcp'):
            return dataset + '-' + key_name
        return f'{dataset}-gcp-{key_name}'

    if '::az' in urn:
        key_name = key_name.replace('az-', '')
        if key_name.startswith(dataset):
            if key_name.startswith(f'{dataset}-azure'):
                return key_name
            else:
                return '-'.join([dataset, 'azure', *key_name.split('-')[1:]])
        if key_name.startswith('azure'):
            return dataset + '-' + key_name
        return f'{dataset}-azure-{key_name}'

    raise ValueError(f'Unrecognised URN: {urn}')


def process_id(*, project_id: str, rtype: str, identifier: str):
    if rtype in membership_types:
        split = identifier.split('/')

        resource = split[0]
        role = "/".join(split[1:-1])
        if rtype == GCP_BUCKET_MEMBERSHIP_TYPE:
            resource = "/".join(split[:2])
            role = "/".join(split[2:-1])
        elif rtype == GCP_SECRET_MEMBERSHIP_TYPE:
            resource = "/".join(split[:4])
            role = "/".join(split[4:-1])
        elif rtype == GCP_REPOSITORY_MEMBERSHIP_TYPE:
            resource = "/".join(split[:6])
            role = "/".join(split[6:-1])

        return " ".join([resource, role, split[-1]])

    if rtype == GCP_BUCKET_OBJECT_TYPE:
        raise ValueError('Cannot migrate this resource')
    if rtype == GCP_BUCKET_TYPE:
        return f'{project_id}/{identifier}'

    return identifier


def migrate_all():

    with open('production.yaml') as f:
        datasets_config = yaml.safe_load(f)

    for filename in glob.glob('Pulumi.*.yaml'):
        dataset = filename.split('.')[1]
        gcp_project_id = datasets_config.get(dataset, {}).get('gcp', {}).get('project')
        assert gcp_project_id, f'Could not get GCP project ID for {dataset}'
        _migrate_stack(gcp_project_id, dataset)

def migrate_stack(dataset):
    with open('production.yaml') as f:
        datasets_config = yaml.safe_load(f)
    gcp_project_id = datasets_config.get(dataset, {}).get('gcp', {}).get('project')
    _migrate_stack(gcp_project_id, dataset)


def _migrate_stack(gcp_project_id, dataset):
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

    parent_map = {}
    for resource in dataset_dict['deployment']['resources']:
        urn = resource['urn']
        if not resource.get('id'):
            print(f'Bad resource: {urn}')
            continue

        if name := process_urn(dataset, urn):
            rtype = resource['type']

            new_resource = {
                    "name": name,
                    "type": rtype,
                    "id": process_id(project_id=gcp_project_id, rtype=rtype, identifier=resource['id']),
                }
            if 'parent' in resource:
                if parent_urn := process_urn(dataset, resource['parent']):
                    parent_map[parent_urn] = parent_urn
                    new_resource['parent'] = resource[parent_urn]
            resources.append(new_resource)
        else:
            print(f'Skipping migrated {urn}')

    with open(f'pulumi-{dataset}-migrated.json', 'w+') as f:
        json.dump({"resources": resources, 'nameTable': parent_map}, f, indent=2)


if __name__ == '__main__':
    migrate_stack('acute-care')
