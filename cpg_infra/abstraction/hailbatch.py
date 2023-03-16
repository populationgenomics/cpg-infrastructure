# pylint: disable=missing-function-docstring,unused-argument
"""
Contains pulumi.dyanmic.ResourceProvider implementations
for Hail Batch Billing Projects and Users.
"""
import os
import json

import requests
import pulumi
import pulumi.dynamic

HAIL_GET_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}'
)
HAIL_CREATE_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}/create'
)
HAIL_CLOSE_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}/close'
)
HAIL_DELETE_USER_FROM_BILLING_PROJECT_PATH = '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}/users/{user}/remove'
# HAIL_CREATE_USER_PATH = '{hail_auth_url}/api/v1alpha/users/{username}/create'
# HAIL_GET_USER_PATH = '{hail_auth_url}/api/v1alpha/users/{username}'

HAIL_ADD_USER_TO_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}/users/{user}/add'
)


def get_hail_batch_token(token_category) -> str:
    """Get Hail batch token from environment or ~/.hail/tokens.json"""
    key = f'HAIL_TOKEN_{token_category.upper()}'
    if hail_token := os.getenv(key):
        return os.getenv(hail_token)

    tokens_path = os.path.expanduser('~/.hail/tokens.json')
    if os.path.exists(tokens_path):
        with open(os.path.expanduser(tokens_path), encoding='utf-8') as f:
            if token := json.load(f).get(token_category):
                return token

    raise ValueError(
        f'Could not find hail batch token for {token_category!r}, you can set the '
        f'environment variable {key}, or you can set the {token_category} token '
        f'in {tokens_path!r}'
    )


class HailBatchBillingProjectProvider(pulumi.dynamic.ResourceProvider):
    """Pulumi provider for a Hail Batch Billing Project"""

    def create(self, inputs):
        hail_auth_token = get_hail_batch_token(inputs['token_category'])
        url = HAIL_CREATE_BILLING_PROJECT_PATH.format(
            hail_batch_url=inputs['batch_uri'], billing_project=inputs['name']
        )
        resp = requests.post(
            url, headers={'Authorization': f'Bearer {hail_auth_token}'}, timeout=60
        )
        resp.raise_for_status()

        return pulumi.dynamic.CreateResult(
            id_=f"{inputs['token_category']}::{inputs['batch_uri']}::{inputs['name']}",
            outs=inputs,
        )

    def read(self, id_: str, props) -> pulumi.dynamic.ReadResult:
        hail_auth_token = get_hail_batch_token(props['token_category'])
        url = HAIL_GET_BILLING_PROJECT_PATH.format(
            hail_batch_url=props['batch_uri'], billing_project=props['name']
        )
        resp = requests.get(
            url, headers={'Authorization': f'Bearer {hail_auth_token}'}, timeout=60
        )

        if resp.status_code == 404:
            return pulumi.dynamic.ReadResult(None, None)

        resp.raise_for_status()

        return pulumi.dynamic.ReadResult(id_=id_, outs=props)

    def delete(self, _id, props):
        """Delete hail batch billing project"""
        hail_auth_token = get_hail_batch_token(props['token_category'])
        url = HAIL_CLOSE_BILLING_PROJECT_PATH.format(
            hail_batch_url=props['batch_uri'], billing_project=props['name']
        )
        resp = requests.post(
            url, headers={'Authorization': f'Bearer {hail_auth_token}'}, timeout=60
        )

        if not resp.ok:
            # more accurate exception
            raise ValueError(
                f'Could not close billing project {props["name"]}: {resp.text}'
            )

    def diff(self, _id, old_inputs, new_inputs):
        replaces = []
        if old_inputs['name'] != new_inputs['name']:
            replaces.append('name')

        if old_inputs['batch_uri'] != new_inputs['batch_uri']:
            replaces.append('batch_uri')

        return pulumi.dynamic.DiffResult(
            len(replaces) > 0, replaces, stables=[], delete_before_replace=False
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
        hail_auth_token = get_hail_batch_token(token_category)
        resp = requests.post(
            url, headers={'Authorization': f'Bearer {hail_auth_token}'}, timeout=60
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

        hail_auth_token = get_hail_batch_token(token_category)
        resp = requests.post(
            url, headers={'Authorization': f'Bearer {hail_auth_token}'}, timeout=60
        )

        if not resp.ok:
            raise ValueError(f'Could not delete user from billing project: {resp.text}')

    def read(self, id_: str, props) -> pulumi.dynamic.ReadResult:
        # TODO: implement read from hail batch to determine if still present
        return pulumi.dynamic.ReadResult(id_=id_, outs=props)


class HailBatchBillingProject(pulumi.dynamic.Resource):
    """Create a Hail Batch Billing Project"""

    def __init__(
        self,
        name: str,
        billing_project_name: pulumi.Input[str],
        batch_uri: pulumi.Input[str],
        token_category: pulumi.Input[str],
        opts: pulumi.ResourceOptions | None = None,
    ):
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
    ):
        args = {
            'billing_project': billing_project,
            'user': user,
        }
        super().__init__(HailBatchBillingProjectMembershipProvider(), name, args, opts)
