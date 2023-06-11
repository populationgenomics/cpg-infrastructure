"""
If Pulumi suddenly loses a bunch of resources but they still exist in the cloud,
this script can be used to generate statements to import them back into state.

It operated on the output of:

    pulumi preview --diff -p 20 --non-interactive --json

It will generate a bunch of statements like:

    pulumi import --generate-code=false --non-interactive --skip-preview --yes \
        $IMPORT_TYPE $NAME $ID $FLAGS

"""
import dataclasses
import functools
import json
from datetime import date

import click
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

    def to_command_line(self):
        """Convert to command line"""
        kwargs_str = ' '.join(f'--{k} {v!r}' for k, v in (self.flags or {}).items())
        return (
            f'pulumi import --generate-code=false --non-interactive --skip-preview '
            f'--yes {self.type!r} {self.name!r} {self.id!r} {kwargs_str}'
        ).strip()


def prepare_id(step_state: dict) -> str | None:
    """Prepare ID from step for import command"""
    type_ = step_state['type']
    inputs = step_state['inputs']

    if type_ == 'gcp:cloudidentity/groupMembership:GroupMembership':
        group = inputs['group']
        email = inputs['preferredMemberKey']['id']
        # if the email doesn't exist in the group, it's a new membership
        # rely heavily on the caching mechanism to avoid us making too many calls
        return get_email_to_membership_id_for_group(group).get(email)

    if type_ == 'gcp:projects/iAMMember:IAMMember':
        project = inputs['project']
        role = inputs['role']
        member = inputs['member']
        return f'{project} {role} {member}'

    if type_ == 'gcp:secretmanager/secretIamMember:SecretIamMember':
        secret_id = inputs['secretId']
        member = inputs['member']
        role = inputs['role']

        return f'{secret_id} {role} {member}'

    if type_ == 'gcp:serviceAccount/iAMMember:IAMMember':
        service_account_id = inputs['serviceAccountId']
        member = inputs['member']
        role = inputs['role']
        return f'{service_account_id} {role} {member}'

    if type_ == 'gcp:storage/bucketIAMMember:BucketIAMMember':
        bucket = inputs['bucket']
        member = inputs['member']
        role = inputs['role']
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
def get_email_to_membership_id_for_group(group_key: str) -> dict[str, str]:
    """
    Get email to membership ID map for a group
    """
    group_memberships = lookup_google_group_members(group_key)
    return {m['preferredMemberKey']['id']: m['name'] for m in group_memberships}


@functools.cache
def lookup_google_group_members(group_key: str):
    """
    Lookup google group members
    """
    if not group_key.startswith('groups/'):
        group_key = f'groups/{group_key}'

    print(f'Getting members for {group_key}')

    groups_svc = get_groups_members_service()
    members = groups_svc.list(parent=group_key).execute()
    return members['memberships']


def main(pulumi_plan_filename: str, output_filename=f'{date.today()}_imports.sh'):
    """
    Driver function for generating import commands
    :return:
    """
    with open(pulumi_plan_filename, encoding='utf-8') as f:
        plan = json.load(f)

    imports = []

    for step in plan['steps']:
        if step['op'] != 'create':
            continue
        state = step.get('newState')
        if not state:
            continue

        if new_id := prepare_id(state):
            imports.append(
                PulumiImport(
                    type=state['type'],
                    name=step['urn'].split('::')[-1],
                    id=new_id,
                    flags={},
                )
            )

    with open(output_filename, 'w', encoding='utf-8') as outfile:
        outfile.writelines(i.to_command_line() for i in imports)


@click.command()
@click.option(
    '--pulumi-plan-filename',
    help='Path to the Pulumi plan file',
    required=True,
)
@click.option(
    '--output-filename',
    help='Path to the output file',
    default=f'{date.today()}_imports.sh',
)
def from_cli(pulumi_plan_filename: str, output_filename: str):
    """
    Entrypoint for CLI
    """
    main(pulumi_plan_filename=pulumi_plan_filename, output_filename=output_filename)


if __name__ == '__main__':
    from_cli()  # pylint: disable=no-value-for-parameter
