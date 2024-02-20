"""
Contains pulumi.dynamic.ResourceProvider implementations for Google Groups Memberships
"""

import textwrap
import time
from functools import cache
from typing import TYPE_CHECKING, Optional, TypedDict
from urllib.parse import urlencode

import google.auth
import googleapiclient.discovery
import pulumi.dynamic
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from pulumi import Input, ResourceOptions

# These don't work at runtime so only include for typing purposes
if TYPE_CHECKING:
    from googleapiclient._apis.cloudidentity.v1.resources import (  # pyright: ignore[reportMissingModuleSource]
        CloudIdentityResource,
    )

MEMBER_LIST_PAGE_SIZE = 100
MEMBERSHIP_CREATE_CONFLICT_STATUS_CODE = 409
MEMBERSHIP_DELETE_ALREADY_DELETED_STATUS_CODE = 404
MEMBERSHIP_DELETE_OPERATION_ABORTED_STATUS_CODE = 409
MEMBERSHIP_CREATE_MAX_RETRIES = 5
MEMBERSHIP_DELETE_MAX_RETRIES = 5


class GroupMember(TypedDict):
    member_name: str
    """
    a path in the format groups/<group_key>/memberships/<member_id
    where member_id is the gcloud id of the member, not the email.
    This is needed when deleting memberships
    """
    member_key: str
    "The group member's email"
    group_key: str
    "alphanumeric group id"


class GroupMemberships:
    member_key_map: dict[str, GroupMember]
    member_name_map: dict[str, GroupMember]

    def __init__(self, members: list[GroupMember]) -> None:
        self.members = members
        self.member_key_map = {m['member_key'].lower(): m for m in self.members}
        self.member_name_map = {m['member_name']: m for m in self.members}

    def find_member_by_key(self, member_key: str) -> GroupMember | None:
        return self.member_key_map.get(member_key.lower(), None)

    def find_member_by_name(self, member_name: str) -> GroupMember | None:
        return self.member_name_map.get(member_name, None)


class GoogleGroupMembershipInputs:
    group_key: Input[str]
    member_key: Input[str]

    def __init__(self, group_key: Input[str], member_key: Input[str]) -> None:
        self.group_key = group_key
        self.member_key = member_key


class GoogleGroupMembershipProviderInputs(TypedDict):
    group_key: str
    member_key: str


class GoogleGroupMembership(pulumi.dynamic.Resource):
    """A Pulumi dynamic resource for Google Group memberships."""

    def __init__(
        self,
        name: str,
        props: GoogleGroupMembershipInputs,
        opts: Optional[ResourceOptions] = None,
    ) -> None:
        super().__init__(
            GoogleGroupMembershipProvider(),
            name,
            {**vars(props)},
            opts,
        )


class GoogleGroupMembershipProvider(pulumi.dynamic.ResourceProvider):
    """A Pulumi dynamic resource provider for Google Groups settings."""

    def create(self, props: GoogleGroupMembershipProviderInputs):
        group_key = props['group_key']
        member_key = props['member_key'].lower()

        members = get_group_memberships(group_key)
        member = members.find_member_by_key(member_key)

        # If the group membership already exists, then don't try and create it
        if member is not None:
            return pulumi.dynamic.CreateResult(id_=member['member_name'], outs=member)

        # Otherwise create it here
        created_member = add_member_to_group(group_key, member_key)

        return pulumi.dynamic.CreateResult(
            id_=created_member['member_name'],
            outs=created_member,
        )

    def read(self, _id: str, props: GroupMember):
        group_key = props['group_key']
        member_key = props['member_key'].lower()
        members = get_group_memberships(group_key)
        member = members.find_member_by_key(member_key)

        # If the member doesn't exist then the group state has got out of sync with
        # gcloud, and will need to be fixed manually
        if member is None:
            raise Exception(
                f"member {member_key} not found in group {group_key}, manual intervention required",
            )

        return pulumi.dynamic.ReadResult(id_=member['member_name'], outs=member)

    def delete(self, id_: str, _props: GroupMember):
        group_key = get_group_from_membership_name(id_)

        members = get_group_memberships(group_key)
        member = members.find_member_by_name(id_)

        # If the member has already been removed then no need to remove
        if member is None:
            return

        remove_member_from_group(id_)

    def diff(
        self,
        _id: str,
        olds: GroupMember,
        news: GoogleGroupMembershipProviderInputs,
    ):
        if (
            olds['member_key'].lower() != news['member_key'].lower()
            or olds['group_key'] != news['group_key']
        ):
            return pulumi.dynamic.DiffResult(
                changes=True,
                replaces=['member_key', 'group_key'],
            )
        return pulumi.dynamic.DiffResult(changes=False)


@cache
def get_credentials():
    """Returns credentials for the Google Cloud Identity API."""
    credentials, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/cloud-identity.groups'],
    )
    credentials.refresh(Request())
    return credentials


def get_groups_service():
    service: CloudIdentityResource = googleapiclient.discovery.build(  # pyright: ignore[reportUnknownMemberType, reportAssignmentType]
        serviceName='cloudidentity',
        version='v1',
        credentials=get_credentials(),
    )
    return service


def get_group_from_membership_name(name: str) -> str:
    """
     Pull group id and membership id from
    format: groups/<group_key>/memberships/<membership_id> str
    """
    return '/'.join(name.split('/')[:2])


@cache
def get_group_memberships(group_key: str) -> GroupMemberships:
    """Returns a set of all members in the given group"""

    service = get_groups_service()

    members: list[GroupMember] = []

    next_page_token = ''

    while True:
        search_query = urlencode(
            {
                'page_size': MEMBER_LIST_PAGE_SIZE,
                'page_token': next_page_token,
            },
        )
        search_members_request = service.groups().memberships().list(parent=group_key)
        param = '&' + search_query
        search_members_request.uri += param
        response = search_members_request.execute()

        if 'memberships' not in response:
            break

        for member in response['memberships']:
            member_key = member.get('preferredMemberKey', {}).get('id')
            # The member name is a path in the format <group_key>/memberships/<member_id>
            # where member_id is the gcloud id of the member, not the email. This is
            # needed when deleting memberships
            member_name = member.get('name')

            if member_key is None:
                raise AttributeError('preferredMemberKey not found')

            if member_name is None:
                raise AttributeError('member name not found')

            members.append(
                {
                    'member_name': member_name,
                    'member_key': member_key.lower(),
                    'group_key': group_key,
                },
            )

        next_page_token = response.get('nextPageToken', '')

        if len(next_page_token) == 0:
            break

    return GroupMemberships(members)


def get_group_memberships_uncached(group_key: str) -> GroupMemberships:
    return get_group_memberships.__wrapped__(group_key)


def add_member_to_group(
    group_key: str,
    member_key: str,
    retry_number: int = 0,
) -> GroupMember:
    """Adds the specified member to the group"""
    service = get_groups_service()

    create_member_request = (
        service.groups()
        .memberships()
        .create(
            parent=group_key,
            body={
                "preferredMemberKey": {"id": member_key},
                "roles": [{'name': 'MEMBER'}],
            },
        )
    )

    try:
        response = create_member_request.execute()
    except HttpError as e:
        # A status code of 409 indicates that the membership already exists
        # this can happen if multiple resources are trying to create the same
        # membership. If this happens then we need to fetch the member again
        # and return it, if the member doesn't exist when fetching then something
        # has gone rather wrong and we need to raise an exception
        if e.status_code == MEMBERSHIP_CREATE_CONFLICT_STATUS_CODE:
            members = get_group_memberships_uncached(group_key)
            member = members.find_member_by_key(member_key)
            if member is None:
                if retry_number >= MEMBERSHIP_CREATE_MAX_RETRIES:
                    raise Exception(
                        f"Max retries exceeded for adding member {member_key} to group {group_key} after receiving 409 error",
                    ) from e

                time.sleep(3)
                pulumi.warn(
                    textwrap.dedent(
                        f"""\
                        gcloud api reported conflict when adding member {member_key}
                        group {group_key} but subsequent check showed that member was
                        not in group. Retrying ({retry_number + 1})
                        """,
                    ),
                )

                return add_member_to_group(group_key, member_key, retry_number + 1)
            return member

        raise e

    if not response.get('done'):
        raise Exception(response.get('error', {}).get('message', 'Unknown Error'))

    member_name = response.get('response', {}).get('name', None)

    if member_name is None:
        raise AttributeError('Member creation response missing member name')

    group_key = get_group_from_membership_name(member_name)

    return {
        'member_name': member_name,
        'member_key': member_key,
        'group_key': group_key,
    }


def remove_member_from_group(
    member_name: str,
    retry_number: int = 0,
) -> None:
    """Removes the specified member from the group"""

    service = get_groups_service()
    remove_member_request = service.groups().memberships().delete(name=member_name)

    try:
        response = remove_member_request.execute()
    except HttpError as e:
        # If the status code is a 404 then the membership was already deleted
        if e.status_code == MEMBERSHIP_DELETE_ALREADY_DELETED_STATUS_CODE:
            return None
        # It seems that these requests can sometimes get 409s too, in that case retry
        if e.status_code == MEMBERSHIP_DELETE_OPERATION_ABORTED_STATUS_CODE:
            if retry_number >= MEMBERSHIP_DELETE_MAX_RETRIES:
                raise Exception(
                    f"Max retries exceeded for removing member {member_name} after receiving 409 error",
                ) from e

            time.sleep(3)
            pulumi.warn(
                textwrap.dedent(
                    f"""\
                        gcloud api reported 409 error on delete operation for member
                        {member_name}. Retrying ({retry_number + 1})
                    """,
                ),
            )

            return remove_member_from_group(member_name, retry_number + 1)

        raise e

    if not response.get('done'):
        raise Exception(response.get('error', {}).get('message', 'Unknown Error'))

    return None
