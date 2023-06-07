import dataclasses
import functools
import json
from collections import defaultdict

import google.auth
import googleapiclient.discovery
from google.auth.transport.requests import Request


@dataclasses.dataclass
class PulumiImport:
    """
    Hold import command with format: # pulumi import [type] [name] [id] [flags]
    """
    type: str
    name: str
    id: str
    flags: dict[str, str] | None = None


def prepare_id(step):
    type_ = step['type']

    if type_ == 'gcp:cloudidentity/groupMembership:GroupMembership':
        # specific ignore
        return None
    elif type_ == 'gcp:projects/iAMMember:IAMMember':
        # example cpg-common/roles/serviceusage.serviceUsageConsumer/group:common-test@populationgenomics.org.au
        project = step['inputs']['project']
        role = step['inputs']['role']
        member = step['inputs']['member']
        return f'{project} {role} {member}'

    elif type_ == 'gcp:secretmanager/secretIamMember:SecretIamMember':
        # projects/analysis-runner/secrets/acute-care-cromwell-test-key/roles/secretmanager.secretAccessor/serviceAccount:acute-care-test-952@hail-295901.iam.gserviceaccount.com
        # example projects/cpg-common/secrets/hail-git-checkout-token/roles/secretmanager.secretAccessor/group:common-test@populationgenomics.org.au
        secret_id = step['inputs']['secretId']
        member = step['inputs']['member']
        role = step['inputs']['role']

        return f'{secret_id} {role} {member}'

    elif type_ == 'gcp:serviceAccount/iAMMember:IAMMember':
        # example projects/cpg-common/serviceAccounts/main-upload@cpg-common.iam.gserviceaccount.com/roles/iam.serviceAccountKeyAdmin/group:common-data-manager@populationgenomics.org.au
        service_account_id = step['inputs']['serviceAccountId']
        member = step['inputs']['member']
        role = step['inputs']['role']
        return f'{service_account_id} {role} {member}'


    elif type_ == 'gcp:storage/bucketIAMMember:BucketIAMMember':
        # example b/cpg-common-main-analysis/organizations/648561325637/roles/StorageObjectAndBucketViewer/group:common-analysis@populationgenomics.org.au
        bucket = step['inputs']['bucket']
        member = step['inputs']['member']
        role = step['inputs']['role']
        return f'{bucket} {role} {member}'

    return None

@functools.cache
def get_groups_credentials():
    """Returns credentials for the Google Groups Settings API."""
    credentials, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/apps.members'],
    )
    credentials.refresh(Request())
    return credentials

def get_groups_members_service():
    """Returns the Google Groups settings service."""
    resource = googleapiclient.discovery.build(
        'cloudidentity', 'v1', credentials=get_groups_credentials()
    )
    return resource.groups().memberships()

@functools.cache
def lookup_google_group_members(group_key: str):
    """
    Lookup google group members
    """
    groups_svc = get_groups_members_service()
    if not group_key.startswith('groups/'):
        group_key = f'groups/{group_key}'
    members = groups_svc.list(parent=group_key).execute()
    return members['memberships']

def main():
    with open('/Users/mfranklin/Desktop/tmp/2023-06-07_pulumi-plan.json') as f:
        plan = json.load(f)

    imports = []
    group_memberships = []

    for step in plan['steps']:
        if step['op'] != 'create':
            continue

        state = step.get('newState')
        if not state:
            print(f'no new state: {step}')
            continue

        if new_id := prepare_id(state):
            imports.append(PulumiImport(
                type=state['type'],
                name=step['urn'].split('::')[-1],
                id=new_id,
                flags={},
            ))

        if state['type'] == 'gcp:cloudidentity/groupMembership:GroupMembership':
            group_memberships.append(step)

    # now we want to look up the group from the groups API
    # map the membership ID from each group, then we can assemble the ID
    group_memberships_by_group = defaultdict(list)
    for step in group_memberships:
        group = step['newState']['inputs']['group']
        group_memberships_by_group[group].append(step)

    for group, memberships in group_memberships_by_group.items():
        print(f'Getting members for {group}')
        members_of_group = lookup_google_group_members(group)
        email_to_id = {m['preferredMemberKey']['id']: m['name'] for m in members_of_group}
        for membership in memberships:
            state = membership['newState']
            email = state['inputs']['preferredMemberKey']['id']
            membership_id = email_to_id[email]
            imports.append(PulumiImport(
                type=state['type'],
                name=membership['urn'].split('::')[-1],
                id=membership_id,
                flags={},
            ))

    with open('2023-06-07_imports.sh', 'w+') as f:
        for i in imports:
            f.write(
                f'pulumi import --generate-code=false --non-interactive --skip-preview '
                f'--yes {i.type!r} {i.name!r} {i.id!r}\n'
            )


if __name__ == '__main__':
    main()
