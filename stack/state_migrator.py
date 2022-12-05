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
    """

    >>> process_urn('common', 'urn:pulumi:common::datasets::azuread:index/group:Group::az-common-access')
    'common-azure-access'
    """
    # "urn:pulumi:acute-care::datasets::gcp:cloudidentity/groupMembership:GroupMembership::hail-service-account-standard-cromwell-access"
    fields = urn.split("::")
    if len(fields) != 4:
        raise ValueError(f'Cannot detect name from URN: {urn}')

    ftype = fields[-2]
    if ftype.startswith('pulumi:providers:'):
        return None

    key_name = fields[-1]

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


membership_types = {
    "gcp:projects/iAMMember:IAMMember",
    "gcp:artifactregistry/repositoryIamMember:RepositoryIamMember",
    "gcp:secretmanager/secretIamMember:SecretIamMember",
    # "gcp:cloudidentity/groupMembership:GroupMembership",
    "gcp:storage/bucketIAMMember:BucketIAMMember",
}
GCP_BUCKET_MEMBERSHIP_TYPE = "gcp:storage/bucketIAMMember:BucketIAMMember"
GCP_SECRET_MEMBERSHIP_TYPE = "gcp:secretmanager/secretIamMember:SecretIamMember"
GCP_REPOSITORY_MEMBERSHIP_TYPE = "gcp:artifactregistry/repositoryIamMember:RepositoryIamMember"

def process_id(rtype: str, identifier: str):
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

    return identifier


def migrate_all():
    for filename in glob.glob('Pulumi.*.yaml'):
        dataset = filename.split('.')[1]
        migrate_stack(dataset)


def migrate_stack(dataset):
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

        urn = resource['urn']
        if not resource.get('id'):
            print(f'Bad resource: {urn}')
            continue

        if name := process_urn(dataset, urn):
            rtype = resource['type']
            resources.append(
                {
                    "name": name,
                    "type": rtype,
                    "id": process_id(rtype, resource['id']),
                }
            )
        else:
            print(f'Skipping migrated {urn}')

    with open(f'pulumi-{dataset}-migrated.json', 'w+') as f:
        json.dump({"resources": resources}, f, indent=2)


if __name__ == '__main__':
    migrate_stack('common')
