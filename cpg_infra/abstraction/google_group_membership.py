"""
Contains pulumi.dynamic.ResourceProvider implementations for Google Groups Memberships
"""

from functools import cache
from typing import TYPE_CHECKING, Any, Optional, TypedDict
from urllib.parse import urlencode

import googleapiclient.discovery
import pulumi.dynamic
from pulumi import Input, ResourceOptions

# These don't work at runtime so only include for typing purposes
if TYPE_CHECKING:
    from googleapiclient._apis.cloudidentity.v1 import (  # type: ignore
        CloudIdentityResource,
    )

MEMBER_LIST_PAGE_SIZE = 100


class GroupMember(TypedDict):

    member_key: str
    "The email of the group member"

    member_name: str
    """
    a path in the format groups/<group_id>/memberships/<member_id
    where member_id is the gcloud id of the member, not the email.
    This is needed when deleting memberships
    """


class GoogleGroupMembershipInputs(object):
    group_id: Input[str]
    member_key: Input[str]

    def __init__(self, group_id: str, member_key: str):
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
        member_key = props['member_key']

        member = find_member_in_group_by_key(group_id, member_key)

        # If the group membership already exists, then don't try and create it
        if member != None:
            return pulumi.dynamic.CreateResult(id_=member['member_name'])

        # Otherwise create it here
        created_member = add_member_to_group(group_id, member_key)

        return pulumi.dynamic.CreateResult(id_=created_member['member_name'])

    def delete(self, id: str, props: Any):
        name_parts = id.split('/')
        group_id = name_parts[1]
        member = find_member_in_group_by_name(group_id, id)

        # If the member has already been removed then no need to remove
        if member == None:
            return

        remove_member_from_group(id)

    def diff(self, id: str, olds: Any, news: Any):
        # There isn't anything to change on a group membership, they are either a member
        # or not, so this can always return false
        return pulumi.dynamic.DiffResult(changes=False)


@cache
def get_groups_service():
    service: CloudIdentityResource = googleapiclient.discovery.build(  # type: ignore
        'cloudidentity', 'v1'
    )
    return service


def find_member_in_group_by_key(group_id: str, member_key: str):
    member_list = list_group_members(group_id)
    return next((m for m in member_list if m['member_key'] == member_key), None)


def find_member_in_group_by_name(group_id: str, member_name: str):
    member_list = list_group_members(group_id)
    return next((m for m in member_list if m['member_name'] == member_name), None)


@cache
def list_group_members(group_id: str) -> list[GroupMember]:
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

            if member_key == None:
                raise AttributeError('preferredMemberKey not found')

            if member_name == None:
                raise AttributeError('member name not found')

            members.append({'member_key': member_key, 'member_name': member_name})

        next_page_token = response.get('nextPageToken', '')

        if len(next_page_token) == 0:
            break

    return members


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

    response = create_member_request.execute()

    if not response.get('done'):
        raise Exception(response.get('error', {}).get('message', 'Unknown Error'))

    member_name = response.get('response', {}).get('name', None)

    if member_name == None:
        raise AttributeError('Member creation response missing member name')

    return {'member_key': member_key, 'member_name': member_name}


def remove_member_from_group(member_name: str):
    """Removes the specified member from the group"""

    service = get_groups_service()
    create_member_request = service.groups().memberships().delete(name=member_name)

    response = create_member_request.execute()

    if not response.get('done'):
        raise Exception(response.get('error', {}).get('message', 'Unknown Error'))
