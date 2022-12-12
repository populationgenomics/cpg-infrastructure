# pylint: disable=too-many-return-statements
"""
Migrate state from multiple stacks to single-stack
"""
import os
import glob
import json
import re
import subprocess

import yaml

from stack.stack_utils import get_pulumi_config_passphrase

if os.path.basename(os.getcwd()) != 'stack':
    raise ValueError('Please run this script in the "stack" subdirectory')

types_to_ignore = {
    'gcp:projects/service:Service',
}

datasets_to_ignore = {'production', 'reference'}
role_regex = re.compile(r'^organizations\/\d+\/')

GCP_BUCKET_OBJECT_TYPE = 'gcp:storage/bucketObject:BucketObject'
GCP_BUCKET_TYPE = 'gcp:storage/bucket:Bucket'
GCP_BUCKET_MEMBERSHIP_TYPE = 'gcp:storage/bucketIAMMember:BucketIAMMember'
GCP_SECRET_MEMBERSHIP_TYPE = 'gcp:secretmanager/secretIamMember:SecretIamMember'
GCP_SERVICE_ACCOUNT_MEMBER_TYPE = 'gcp:serviceAccount/iAMMember:IAMMember'
GCP_CLOUD_RUN_MEMBERSHIP_TYPE = 'gcp:cloudrun/iamMember:IamMember'
GCP_REPOSITORY_MEMBERSHIP_TYPE = (
    'gcp:artifactregistry/repositoryIamMember:RepositoryIamMember'
)
GCP_IAM_MEMBER_TYPE = 'gcp:serviceAccount:IAMMember'
GCP_GROUP_TYPE = 'gcp:cloudidentity/group:Group'
AZURE_BLOB_CONTAINER_TYPE = 'azure-native:storage:BlobContainer'
AZURE_GROUP_TYPE = 'azuread:index/group:Group'


membership_types = {
    'gcp:projects/iAMMember:IAMMember',
    GCP_REPOSITORY_MEMBERSHIP_TYPE,
    GCP_SECRET_MEMBERSHIP_TYPE,
    GCP_BUCKET_MEMBERSHIP_TYPE,
    GCP_IAM_MEMBER_TYPE,
    GCP_CLOUD_RUN_MEMBERSHIP_TYPE,
    GCP_SERVICE_ACCOUNT_MEMBER_TYPE,
}


def process_urn_into_name(dataset: str, urn: str) -> str | None:
    """
    Process URN into name

    >>> process_urn_into_name('common', 'urn:pulumi:common::datasets::azuread:index/group:Group::az-common-access')
    'common-azure-access-group'

    >>> process_urn_into_name('thousand-genomes', 'urn:pulumi:thousand-genomes::datasets::gcp:secretmanager/secret:Secret::thousand-genomes-cromwell-test-key')
    'thousand-genomes-gcp-cromwell-test-key'
    """
    # 'urn:pulumi:acute-care::datasets::gcp:cloudidentity/groupMembership:GroupMembership::hail-service-account-standard-cromwell-access'

    forbidden_urns = [
        f'urn:pulumi:{dataset}::datasets::pulumi:pulumi:Stack::',
        f'urn:pulumi:{dataset}::datasets::pulumi:pulumi:StackReference::',
    ]

    if any(urn.startswith(fbdn) for fbdn in forbidden_urns):
        return None

    fields = urn.split('::')
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
            return '-'.join([dataset, 'gcp', key_name.removeprefix(dataset + '-')])
        if key_name.startswith('gcp'):
            return dataset + '-' + key_name
        return f'{dataset}-gcp-{key_name}'

    if '::az' in urn:
        key_name = key_name.replace('az-', '')
        if key_name.startswith(dataset):
            if key_name.startswith(f'{dataset}-azure'):
                return key_name
            return '-'.join([dataset, 'azure', key_name.removeprefix(dataset + '-')])
        if key_name.startswith('azure'):
            return dataset + '-' + key_name
        return f'{dataset}-azure-{key_name}'

    raise ValueError(f'Unrecognised URN: {urn}')


def process_id(*, project_id: str, rtype: str, identifier: str):
    """
    Process ID, because for some reason the identifier pulumi exports
    from state is not in the correct format to import.

    There are special cases for memberships too
    """
    if rtype in membership_types:
        split = identifier.split('/')

        resource_bound = 1
        role_bound = -1

        if rtype in (GCP_BUCKET_MEMBERSHIP_TYPE, GCP_IAM_MEMBER_TYPE):
            resource_bound = 2
        elif rtype == GCP_SECRET_MEMBERSHIP_TYPE:
            resource_bound = 4
        elif rtype == GCP_REPOSITORY_MEMBERSHIP_TYPE:
            resource_bound = 6
        elif rtype == GCP_CLOUD_RUN_MEMBERSHIP_TYPE:
            resource_bound = 7
        elif rtype == GCP_SERVICE_ACCOUNT_MEMBER_TYPE:
            resource_bound = 4

        #   (thousand-genomes-gcp-cromwell-runner-standard-service-account-user):
        # projects/thousand-genomes/serviceAccounts/cromwell-standard@thousand-genomes.iam.gserviceaccount.com/roles/iam.serviceAccountUser/serviceaccount:cromwell-runner@cromwell-305305.iam.gserviceaccount.com: Wrong number of parts to Member id [projects/thousand-genomes/serviceAccounts/cromwell-standard@thousand-genomes.iam.gserviceaccount.com/roles/iam.serviceAccountUser/serviceaccount:cromwell-runner@cromwell-305305.iam.gserviceaccount.com]; expected 'resource_name role member [condition_title]'.

        resource = '/'.join(split[:resource_bound])
        role = '/'.join(split[resource_bound:role_bound])

        if rtype == GCP_BUCKET_MEMBERSHIP_TYPE:
            resource = resource.removeprefix('b/')

        if role.startswith('subscriptions'):
            # this imports, just ends up applying an update
            role = '/' + role

        if not role.startswith('organizations/648561325637'):
            # don't replace our organization roles, because we do
            # have some custom roles
            role = role_regex.sub('/', role)
        return ' '.join([resource, role, split[-1]])

    if rtype == GCP_BUCKET_OBJECT_TYPE:
        raise ValueError('Cannot migrate this resource')
    if rtype == GCP_BUCKET_TYPE:
        return f'{project_id}/{identifier}'

    return identifier


def migrate_all():
    """
    Loop through existing Pulumi stacks, and migrate one by one
    """
    with open('production.yaml', encoding='utf-8') as f:
        datasets_config = yaml.safe_load(f)

    resources = []
    for dataset in datasets_config:
        gcp_project_id = datasets_config.get(dataset, {}).get('gcp', {}).get('project')
        assert gcp_project_id, f'Could not get GCP project ID for {dataset}'
        resources.extend(_migrate_stack(gcp_project_id, dataset))

    with open(f'pulumi-production-migrated.json', 'w+', encoding='utf-8') as f:
        json.dump({'resources': resources}, f, indent=2)


def migrate_stack(dataset):
    """Migrate single stack, look up the project_id"""
    with open('production.yaml', encoding='utf-8') as f:
        datasets_config = yaml.safe_load(f)
    gcp_project_id = datasets_config.get(dataset, {}).get('gcp', {}).get('project')
    return _migrate_stack(gcp_project_id, dataset)


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

    with open(state_filename, encoding='utf-8') as f:
        dataset_dict = json.load(f)

    resources = []

    for resource in dataset_dict['deployment']['resources']:
        urn = resource['urn']

        if name := process_urn_into_name(dataset, urn):
            rtype = resource['type']

            if not resource.get('id'):
                print(f'Bad resource: {urn}')
                continue

            new_resource = {
                'name': name,
                'type': rtype,
                'id': process_id(
                    project_id=gcp_project_id, rtype=rtype, identifier=resource['id']
                ),
            }

            resources.append(new_resource)
        else:
            print(f'Skipping migrated {urn}')

    with open(f'pulumi-{dataset}-migrated.json', 'w+', encoding='utf-8') as f:
        json.dump({'resources': resources}, f, indent=2)

    return resources


def _test():

    with open(f'pulumi-production-test-migrated.json', 'w+', encoding='utf-8') as f:
        json.dump(
            {
                'resources': [
                    *migrate_stack('common'),
                    *migrate_stack('thousand-genomes'),
                ]
            },
            f,
            indent=2,
        )


if __name__ == '__main__':
    migrate_all()
