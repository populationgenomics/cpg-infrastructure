#!/usr/bin/env python3

"""Removes group memberships matching a given regular expression."""

from urllib.parse import urlencode
import sys
import regex
import googleapiclient.discovery

CUSTOMER_ID = 'C010ys3gt'
PAGE_SIZE = 1000  # How many results to fetch per request.


def list_all_groups(service):
    """Returns a dictionary of all groups in the domain, mapping addresses to group resource names."""
    groups = {}
    next_page_token = ''
    while True:
        search_query = urlencode(
            {
                'query': f'parent=="customerId/{CUSTOMER_ID}" && "cloudidentity.googleapis.com/groups.discussion_forum" in labels',
                'page_size': PAGE_SIZE,
                'page_token': next_page_token,
            }
        )
        search_group_request = service.groups().search()
        param = '&' + search_query
        search_group_request.uri += param
        response = search_group_request.execute()

        if 'groups' not in response:
            break

        for group in response['groups']:
            groups[group['groupKey']['id']] = group['name']

        if 'nextPageToken' in response:
            next_page_token = response['nextPageToken']
        else:
            next_page_token = ''

        if len(next_page_token) == 0:
            break

    return groups


def list_members(service, group_name):
    """Returns a dictionary of all members in the given group, mapping addresses to member resource names."""
    members = {}
    next_page_token = ''
    while True:
        search_query = urlencode(
            {
                'page_size': PAGE_SIZE,
                'page_token': next_page_token,
            }
        )
        search_members_request = service.groups().memberships().list(parent=group_name)
        param = '&' + search_query
        search_members_request.uri += param
        response = search_members_request.execute()

        if 'memberships' not in response:
            break

        for member in response['memberships']:
            members[member['preferredMemberKey']['id']] = member['name']

        if 'nextPageToken' in response:
            next_page_token = response['nextPageToken']
        else:
            next_page_token = ''

        if len(next_page_token) == 0:
            break

    return members


def main():
    """Main entrypoint."""
    if len(sys.argv) != 2:
        print('Usage: submit.py <member_regex>')
        sys.exit(1)

    member_regex = regex.compile(sys.argv[1])

    service = googleapiclient.discovery.build('cloudidentity', 'v1')
    groups = list_all_groups(service)
    for group_address, group_name in groups.items():
        print(f'Processing {group_address} ({group_name})')
        members = list_members(service, group_name)
        for member_address, member_name in members.items():
            if member_regex.fullmatch(member_address):
                print(f'  Deleting {member_address} ({member_name})')
                service.groups().memberships().delete(name=member_name).execute()


if __name__ == '__main__':
    main()
