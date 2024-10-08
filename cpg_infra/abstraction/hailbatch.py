# flake8: noqa: ANN001,ARG002,PLR2004 ERA001
"""
Contains pulumi.dyanmic.ResourceProvider implementations
for Hail Batch Billing Projects and Users.
"""

import json
import os
from functools import lru_cache
from time import sleep

import pulumi
import pulumi.dynamic
import requests

HAIL_GET_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}'
)
HAIL_CREATE_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}/create'
)
HAIL_REOPEN_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}/reopen'
)
HAIL_CLOSE_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}/close'
)
HAIL_DELETE_USER_FROM_BILLING_PROJECT_PATH = '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}/users/{user}/remove'


HAIL_ADD_USER_TO_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}/users/{user}/add'
)

HAIL_GET_USER = '{hail_batch_url}/api/v1alpha/users/{username}'
HAIL_CREATE_USER_PATH = '{hail_batch_url}/api/v1alpha/users/{username}/create'


@lru_cache(maxsize=4)
def get_hail_batch_auth_headers(token_category: str, batch_uri: str) -> dict[str, str]:
    token = get_hail_batch_auth_token(token_category)
    headers = {'Authorization': f'Bearer {token}'}
    # If this is a dev service then need an extra header
    if batch_uri.startswith('https://internal.hail'):
        headers['X-Hail-Internal-Authorization'] = f'Bearer {token}'

    return headers


def get_hail_batch_auth_token(token_category: str) -> str:
    """Get Hail batch token from environment or ~/.hail/tokens.json"""
    key = f'HAIL_TOKEN_{token_category.upper()}'
    if hail_token := os.getenv(key):
        return hail_token

    tokens_path = os.path.expanduser('~/.hail/tokens.json')
    if os.path.exists(tokens_path):
        with open(os.path.expanduser(tokens_path), encoding='utf-8') as f:
            if token := json.load(f).get(token_category):
                return token

    raise ValueError(
        f'Could not find hail batch token for {token_category!r}, you can set the '
        f'environment variable {key}, or you can set the {token_category} token '
        f'in {tokens_path!r}',
    )


def get_hail_batch_user(
    username: str,
    token_category: str,
    batch_uri: str,
) -> dict | None:
    """
    Get a Hail Batch User
    :return: hail user dictionary if it exists
    """

    url = HAIL_GET_USER.format(username=username, hail_batch_url=batch_uri)
    headers = get_hail_batch_auth_headers(token_category, batch_uri)
    resp = requests.get(url, headers=headers, timeout=60)

    if resp.status_code == 404:
        return None

    resp.raise_for_status()

    return resp.json()


def get_hail_batch_billing_project(name: str, token_category: str, batch_uri: str):
    """Get a Hail Batch Billing Project"""
    hail_auth_headers = get_hail_batch_auth_headers(token_category, batch_uri)
    url = HAIL_GET_BILLING_PROJECT_PATH.format(
        hail_batch_url=batch_uri,
        billing_project=name,
    )
    resp = requests.get(
        url,
        headers=hail_auth_headers,
        timeout=60,
    )
    if resp.status_code == 404:
        return None
    if resp.status_code == 403 and 'Unknown Hail Batch' in resp.text:
        # Ignore 403: Unknown Hail Batch billing project <project>
        return None

    resp.raise_for_status()

    return resp.json()


class HailBatchUserProvider(pulumi.dynamic.ResourceProvider):
    def create(self, props) -> pulumi.dynamic.CreateResult:
        batch_uri = props['batch_uri']
        username = props['username']
        token_category = props['token_category']

        # check if it exists
        user_obj = get_hail_batch_user(
            username,
            token_category=token_category,
            batch_uri=batch_uri,
        )

        if user_obj:
            return pulumi.dynamic.CreateResult(
                id_=f'{token_category}::{batch_uri}::{username}',
                outs={
                    'cloud_id': user_obj.get('hail_identity'),
                    'username': username,
                    'batch_uri': batch_uri,
                    'token_category': token_category,
                },
            )

        url = HAIL_CREATE_USER_PATH.format(
            hail_batch_url=batch_uri,
            username=username,
        )
        hail_auth_headers = get_hail_batch_auth_headers(token_category, batch_uri)
        resp = requests.post(
            url,
            headers=hail_auth_headers,
            timeout=60,
            data=json.dumps(
                {
                    'user': username,
                    'login_id': None,
                    'is_developer': False,
                    'is_service_account': True,
                },
            ),
        )

        resp.raise_for_status()

        cloud_id = None
        # now we have to wait for the user to be created
        # this can take quite some time as the hail job to sync users to GCP only
        # runs every 5 seconds
        attempts = 7
        while not cloud_id and attempts > 0:
            user_obj = get_hail_batch_user(
                username,
                token_category=token_category,
                batch_uri=batch_uri,
            )
            if user_obj and user_obj.get('hail_identity'):
                cloud_id = user_obj.get('hail_identity')
            else:
                attempts -= 1
                sleep(5)

        if not cloud_id:
            raise Exception(f'Hail user {username} did not create in time')

        return pulumi.dynamic.CreateResult(
            id_=f'{token_category}::{batch_uri}::{username}',
            outs={
                'cloud_id': cloud_id,
                'username': username,
                'batch_uri': batch_uri,
                'token_category': token_category,
            },
        )

    def read(self, id_: str, props) -> pulumi.dynamic.ReadResult:
        username = props['username']
        token_category = props['token_category']
        batch_uri = props['batch_uri']

        user_obj = get_hail_batch_user(
            username,
            token_category=token_category,
            batch_uri=batch_uri,
        )

        if not user_obj:
            return pulumi.dynamic.ReadResult(None, {})

        return pulumi.dynamic.ReadResult(
            id_=id_,
            outs={
                'cloud_id': user_obj.get('hail_identity'),
                'username': username,
                'batch_uri': batch_uri,
                'token_category': token_category,
            },
        )

    def delete(self, _id, props):
        """Don't delete users, it's very painful to bring them back"""

    def diff(self, _id, old_inputs, new_inputs):
        replaces = []
        if old_inputs['username'] != new_inputs['username']:
            replaces.append('username')

        if old_inputs['batch_uri'] != new_inputs['batch_uri']:
            replaces.append('batch_uri')

        return pulumi.dynamic.DiffResult(
            len(replaces) > 0,
            replaces,
            stables=[],
            delete_before_replace=False,
        )


class HailBatchBillingProjectProvider(pulumi.dynamic.ResourceProvider):
    """Pulumi provider for a Hail Batch Billing Project"""

    def create(self, props) -> pulumi.dynamic.CreateResult:
        batch_uri = props['batch_uri']
        name = props['name']
        token_category = props['token_category']

        previous_result = get_hail_batch_billing_project(
            name,
            token_category,
            batch_uri,
        )
        hail_auth_headers = get_hail_batch_auth_headers(token_category, batch_uri)

        if previous_result and previous_result['status'] == 'closed':
            # reopen instead of create
            url = HAIL_REOPEN_BILLING_PROJECT_PATH.format(
                hail_batch_url=batch_uri,
                billing_project=name,
            )
            resp = requests.post(
                url,
                headers=hail_auth_headers,
                timeout=60,
            )
            resp.raise_for_status()
        else:
            url = HAIL_CREATE_BILLING_PROJECT_PATH.format(
                hail_batch_url=batch_uri,
                billing_project=name,
            )
            resp = requests.post(
                url,
                headers=hail_auth_headers,
                timeout=60,
            )
            resp.raise_for_status()

        return pulumi.dynamic.CreateResult(
            id_=f'{token_category}::{batch_uri}::{name}',
            outs=props,
        )

    def read(self, id_: str, props) -> pulumi.dynamic.ReadResult:
        resp = get_hail_batch_billing_project(
            props['name'],
            props['token_category'],
            props['batch_uri'],
        )

        if not resp:
            return pulumi.dynamic.ReadResult(None, {})

        if resp['status'] == 'closed':
            return pulumi.dynamic.ReadResult(None, {})

        return pulumi.dynamic.ReadResult(id_=id_, outs=props)

    def delete(self, _id, props):
        """Delete hail batch billing project"""
        batch_uri = props['batch_uri']
        hail_auth_headers = get_hail_batch_auth_headers(
            props['token_category'],
            batch_uri,
        )
        url = HAIL_CLOSE_BILLING_PROJECT_PATH.format(
            hail_batch_url=batch_uri,
            billing_project=props['name'],
        )
        resp = requests.post(
            url,
            headers=hail_auth_headers,
            timeout=60,
        )

        if not resp.ok:
            # more accurate exception
            raise ValueError(
                f'Could not close billing project {props["name"]}: {resp.text}',
            )

    def diff(self, _id, old_inputs, new_inputs):
        replaces = []
        if old_inputs['name'] != new_inputs['name']:
            replaces.append('name')

        if old_inputs['batch_uri'] != new_inputs['batch_uri']:
            replaces.append('batch_uri')

        return pulumi.dynamic.DiffResult(
            len(replaces) > 0,
            replaces,
            stables=[],
            delete_before_replace=False,
        )


class HailBatchBillingProjectMembershipProvider(pulumi.dynamic.ResourceProvider):
    """Pulumi provider for membership to a Hail Batch Billing Project"""

    def create(self, props) -> pulumi.dynamic.CreateResult:
        billing_project = props['billing_project']

        if isinstance(billing_project, HailBatchBillingProject):
            billing_project_id = billing_project.id
        else:
            billing_project_id = billing_project

        user = props['user']

        token_category, batch_uri, billing_project_name = billing_project_id.split('::')
        url = HAIL_ADD_USER_TO_BILLING_PROJECT_PATH.format(
            hail_batch_url=batch_uri,
            billing_project=billing_project_name,
            user=user,
        )

        hail_auth_headers = get_hail_batch_auth_headers(token_category, batch_uri)
        resp = requests.post(
            url,
            headers=hail_auth_headers,
            timeout=60,
        )
        resp.raise_for_status()

        return pulumi.dynamic.CreateResult(
            id_=f'{token_category}::{batch_uri}::{billing_project_name}::{user}',
            outs=props,
        )

    def diff(self, _id: str, _olds, _news) -> pulumi.dynamic.DiffResult:
        replaces = []

        if _olds['billing_project'] != _news['billing_project']:
            replaces.append('billing_project')
        if _olds['user'] != _news['user']:
            replaces.append('user')

        return pulumi.dynamic.DiffResult(
            changes=len(replaces) > 0,
            replaces=replaces,
            delete_before_replace=len(replaces) > 0,
        )

    def delete(self, _id: str, _props) -> None:
        bp_components = _props['billing_project'].split('::')
        token_category, batch_uri, billing_project_name = bp_components
        url = HAIL_DELETE_USER_FROM_BILLING_PROJECT_PATH.format(
            hail_batch_url=batch_uri,
            billing_project=billing_project_name,
            user=_props['user'],
        )

        hail_auth_headers = get_hail_batch_auth_headers(token_category, batch_uri)
        resp = requests.post(
            url,
            headers=hail_auth_headers,
            timeout=60,
        )

        if not resp.ok:
            raise ValueError(f'Could not delete user from billing project: {resp.text}')

    def read(self, id_: str, props) -> pulumi.dynamic.ReadResult:
        bp_components = props['billing_project'].split('::')
        token_category, batch_uri, billing_project_name = bp_components
        resp = get_hail_batch_billing_project(
            billing_project_name,
            token_category,
            batch_uri,
        )

        if not resp:
            return pulumi.dynamic.ReadResult(None, {})

        if resp['status'] == 'closed':
            return pulumi.dynamic.ReadResult(None, {})

        user = props['user']
        if user in resp['users']:
            return pulumi.dynamic.ReadResult(id_=id_, outs=props)

        return pulumi.dynamic.ReadResult(None, {})


class HailBatchUser(pulumi.dynamic.Resource):
    """Create a Hail Batch User"""

    cloud_id: pulumi.Output[str]
    username: pulumi.Output[str]

    def __init__(
        self,
        name: str,
        username: pulumi.Input[str],
        batch_uri: pulumi.Input[str],
        token_category: pulumi.Input[str],
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        args = {
            'username': username,
            'batch_uri': batch_uri,
            'token_category': token_category,
            'cloud_id': None,
        }
        super().__init__(HailBatchUserProvider(), name, args, opts)


class HailBatchBillingProject(pulumi.dynamic.Resource):
    """Create a Hail Batch Billing Project"""

    def __init__(
        self,
        name: str,
        billing_project_name: pulumi.Input[str],
        batch_uri: pulumi.Input[str],
        token_category: pulumi.Input[str],
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        args = {
            'name': billing_project_name,
            'batch_uri': batch_uri,
            'token_category': token_category,
        }
        super().__init__(HailBatchBillingProjectProvider(), name, args, opts)


class HailBatchBillingProjectMembership(pulumi.dynamic.Resource):
    """Create a membership to a Hail Batch Billing Project"""

    def __init__(
        self,
        name: str,
        billing_project: pulumi.Input[HailBatchBillingProject],
        user: pulumi.Input[str],
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        args = {
            'billing_project': billing_project,
            'user': user,
        }
        super().__init__(HailBatchBillingProjectMembershipProvider(), name, args, opts)
