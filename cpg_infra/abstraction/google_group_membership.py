"""
Contains pulumi.dynamic.ResourceProvider implementations for Google Groups Memberships
"""

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


class GroupMember(TypedDict):
    member_name: str
    """
    a path in the format groups/<group_id>/memberships/<member_id
    where member_id is the gcloud id of the member, not the email.
    This is needed when deleting memberships
    """
    member_key: str
    "The group member's email"
    group_id: str
    "alphanumeric group id"
    membership_id: str
    "numeric membership id"


class GroupMemberships:
    member_key_map: dict[str, GroupMember]
    member_name_map: dict[str, GroupMember]

    def __init__(self, members: list[GroupMember]) -> None:
        self.members = members
        self.member_key_map = {m['member_key']: m for m in self.members}
        self.member_name_map = {m['member_name']: m for m in self.members}

    def find_member_by_key(self, member_key: str) -> GroupMember | None:
        return self.member_key_map.get(member_key, None)

    def find_member_by_name(self, member_name: str) -> GroupMember | None:
        return self.member_name_map.get(member_name, None)


class GoogleGroupMembershipInputs:
    group_id: Input[str]
    member_key: Input[str]

    def __init__(self, group_id: Input[str], member_key: Input[str]) -> None:
        self.group_id = group_id
        self.member_key = member_key


class GoogleGroupMembershipProviderInputs(TypedDict):
    group_id: str
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
        group_id = props['group_id']
        member_key = props['member_key'].lower()

        members = get_group_memberships(group_id)
        member = members.find_member_by_key(member_key)

        # If the group membership already exists, then don't try and create it
        if member is not None:
            return pulumi.dynamic.CreateResult(id_=member['member_name'], outs=member)

        # Otherwise create it here
        created_member = add_member_to_group(group_id, member_key)

        return pulumi.dynamic.CreateResult(
            id_=created_member['member_name'],
            outs=created_member,
        )

    def delete(self, id_: str, _props: GroupMember):
        group_id, _membership_id = member_details_from_name(id_)

        members = get_group_memberships(group_id)
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
            olds['member_key'] != news['member_key']
            or olds['group_id'] != news['group_id']
        ):
            return pulumi.dynamic.DiffResult(
                changes=True,
                replaces=['member_key', 'group_id'],
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
    service: CloudIdentityResource = (
        googleapiclient.discovery.build(  # pyright: ignore[reportUnknownMemberType, reportAssignmentType]
            serviceName='cloudidentity',
            version='v1',
            credentials=get_credentials(),
        )
    )
    return service


def member_details_from_name(name: str):
    "pull group id and membership id from groups/<group_id>/memberships/<membership_id> str"
    _g, group_id, _m, membership_id = name.split('/')
    return (group_id, membership_id)


@cache
def get_group_memberships(group_id: str) -> GroupMemberships:
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
        search_members_request = (
            service.groups().memberships().list(parent=f"groups/{group_id}")
        )
        param = '&' + search_query
        search_members_request.uri += param
        response = search_members_request.execute()

        if 'memberships' not in response:
            break

        for member in response['memberships']:
            member_key = member.get('preferredMemberKey', {}).get('id')
            # The member name is a path in the format groups/<group_id>/memberships/<member_id>
            # where member_id is the gcloud id of the member, not the email. This is
            # needed when deleting memberships
            member_name = member.get('name')

            if member_key is None:
                raise AttributeError('preferredMemberKey not found')

            if member_name is None:
                raise AttributeError('member name not found')

            group_id, membership_id = member_details_from_name(member_name)

            members.append(
                {
                    'member_name': member_name,
                    'member_key': member_key,
                    'group_id': group_id,
                    'membership_id': membership_id,
                },
            )

        next_page_token = response.get('nextPageToken', '')

        if len(next_page_token) == 0:
            break

    return GroupMemberships(members)


def get_group_memberships_uncached(group_id: str) -> GroupMemberships:
    return get_group_memberships.__wrapped__(group_id)


def add_member_to_group(group_id: str, member_key: str) -> GroupMember:
    """Adds the specified member to the group"""
    service = get_groups_service()

    create_member_request = (
        service.groups()
        .memberships()
        .create(
            parent=f"groups/{group_id}",
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
            members = get_group_memberships_uncached(group_id)
            member = members.find_member_by_key(member_key)
            if member is None:
                raise Exception(
                    "Member was already created but didn't exist upon checking",
                ) from e
            return member

        raise e

    if not response.get('done'):
        raise Exception(response.get('error', {}).get('message', 'Unknown Error'))

    member_name = response.get('response', {}).get('name', None)

    if member_name is None:
        raise AttributeError('Member creation response missing member name')

    group_id, membership_id = member_details_from_name(member_name)

    return {
        'member_name': member_name,
        'member_key': member_key,
        'group_id': group_id,
        'membership_id': membership_id,
    }


def remove_member_from_group(member_name: str):
    """Removes the specified member from the group"""

    service = get_groups_service()
    remove_member_request = service.groups().memberships().delete(name=member_name)

    try:
        response = remove_member_request.execute()
    except HttpError as e:
        # If the status code is a 404 then the membership was already deleted
        if e.status_code == MEMBERSHIP_DELETE_ALREADY_DELETED_STATUS_CODE:
            return
        raise e

    if not response.get('done'):
        raise Exception(response.get('error', {}).get('message', 'Unknown Error'))
