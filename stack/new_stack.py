#!/usr/bin/env python3
# pylint: disable=invalid-name
"""
Create GCP project + budget + hail service accounts + stack file for Pulumi

requirements:
    - click pyyaml sample-metadata google-cloud-billing-budgets
    - kubectl needs to be installed in the environment
        (due to subprocess call)

Example usage:

    cd stack
    DATASET="my-dataset"
    python new_stack.py \
        --dataset $DATASET \
        --add-as-seqr-dependency \
        --configure-hail-batch-project \
        --deploy-stack \
        --generate-service-account-key
"""

# pylint: disable=unreachable,too-many-arguments,no-name-in-module,import-error,too-many-lines
import os
import random
import re
import json
import logging
import subprocess
import time
from collections import defaultdict
from enum import Enum

import yaml
import click
import requests
from google.cloud.billing.budgets_v1.services.budget_service import (
    BudgetServiceClient,
)
from google.cloud.billing.budgets_v1 import (
    Budget,
    BudgetAmount,
    Filter,
    ThresholdRule,
    NotificationsRule,
    CalendarPeriod,
)
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import GoogleAPICallError
from google.type.money_pb2 import Money
from stack_utils import get_pulumi_config_passphrase  # pylint: disable=import-error
from sample_metadata.apis import ProjectApi


class Cloud(Enum):
    """Clouds that we can create a new stack on"""

    AZURE = 'azure'
    GCP = 'gcp'


TRUTHY_VALUES = ('y', '1', 't')
DATASET_REGEX = r'^[a-z][a-z0-9-]{1,15}[a-z]$'
GCP_PROJECT_REGEX = r'^[a-z0-9-_]{6,30}$'
TIMEOUT = 10  # seconds

ORGANIZATION_ID = '648561325637'
BILLING_ACCOUNT_ID = '01D012-20A6A2-CBD343'
BILLING_PROJECT_ID = 'billing-admin-290403'
GCP_HAIL_PROJECT = 'hail-295901'
AZURE_HAIL_SUBSCRIPTION = '2a974991-7c24-48c2-871f-5f7969a2b0c0'
BILLING_AGGREGATOR_USERNAME = 'aggregate-billing'

AZURE_POPGEN_TENANT = 'a744336e-0ec4-40f1-891f-6c8ccaf8e267'
AZURE_ENROLLMENT_ACCOUNT_ID = (
    '/providers/Microsoft.Billing/billingAccounts/75463289/enrollmentAccounts/302404'
)
AZURE_CPG_MANAGEMENT_GROUP = (
    '/providers/Microsoft.Management/managementGroups/centre_for_population_genomics'
)
AZURE_LOCATION = 'australiaeast'

HAIL_AUTH_URL = {
    Cloud.GCP: 'https://auth.hail.populationgenomics.org.au',
    Cloud.AZURE: 'https://auth.azhail.popgen.rocks',
}
HAIL_BATCH_URL = {
    Cloud.GCP: 'https://batch.hail.populationgenomics.org.au',
    Cloud.AZURE: 'https://batch.azhail.popgen.rocks',
}

HAIL_CREATE_USER_PATH = '{hail_auth_url}/api/v1alpha/users/{username}/create'
HAIL_GET_USER_PATH = '{hail_auth_url}/api/v1alpha/users/{username}'
HAIL_GET_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}'
)
HAIL_CREATE_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}/create'
)
HAIL_ADD_USER_TO_BILLING_PROJECT_PATH = (
    '{hail_batch_url}/api/v1alpha/billing_projects/{billing_project}/users/{user}/add'
)


logging.basicConfig(level=logging.INFO)


@click.command()
@click.option('--dataset', required=True)
@click.option(
    '--clouds',
    help='Cloud platforms to setup stack file for deployment',
    default=[Cloud.GCP.value],
    multiple=True,
    type=click.Choice([e.value for e in Cloud]),
)
@click.option(
    '--gcp-project-id', required=False, help='If different to the dataset name'
)
@click.option('--do-not-create-hail-service-accounts', required=False, is_flag=True)
@click.option('--add-as-seqr-dependency', required=False, is_flag=True)
@click.option('--configure-hail-batch-project', required=False, is_flag=True)
@click.option('--deploy-stack', required=False, is_flag=True, help='Runs `pulumi up`')
@click.option('--generate-service-account-key', required=False, is_flag=True)
@click.option('--add-random-digits-to-gcp-id', required=False, is_flag=True)
@click.option(
    '--budget',
    help='Monthly budget in whole AUD, order corresponds to cloud',
    default=[100],
    multiple=True,
)
@click.option('--create-release-buckets', required=False, is_flag=True)
def main(
    dataset: str,
    clouds: list[str],
    gcp_project_id: str = None,
    do_not_create_hail_service_accounts=False,
    add_as_seqr_dependency=False,
    configure_hail_batch_project=False,
    deploy_stack=False,
    generate_service_account_key=False,
    add_random_digits_to_gcp_id=False,
    budget=None,
    create_release_buckets=False,
):
    """Function that coordinates creating a project"""

    clouds = [Cloud(c) for c in clouds]
    dataset = dataset.lower()

    if len(budget) == len(clouds):
        budgets = dict(zip(clouds, budget))
    elif len(budget) == 1:
        budgets = {c: budget[0] for c in clouds}
    else:
        raise ValueError(
            f'The budget array length must be exactly 1 or exactly len(clouds)'
        )

    # TODO: eventually remove when gcp_project_id not required by Metamist
    assert Cloud.GCP in clouds

    _gcp_project = gcp_project_id
    if not gcp_project_id:
        suffix = ''
        if add_random_digits_to_gcp_id:
            suffix = '-' + str(random.randint(100000, 999999))
        _gcp_project = dataset + suffix

    if len(dataset) > 17:
        raise ValueError(
            f'The dataset length must be less than (or equal to) 17 characters (got {len(dataset)})'
        )

    if not re.fullmatch(GCP_PROJECT_REGEX, _gcp_project):
        components = [
            f'The GCP project ID "{_gcp_project}" must be between 6 and 30 characters'
        ]
        if len(_gcp_project) < 6:
            components.append('consider adding the --add-random-digits-to-gcp-id flag')

        raise ValueError(', '.join(components))

    match = re.fullmatch(DATASET_REGEX, dataset)
    if not match:
        raise ValueError(f'Expected dataset {dataset} to match {DATASET_REGEX}.')

    if os.path.basename(os.getcwd()) != 'stack':
        raise Exception(
            f'You should run this in the analysis-runner/stack directory, got {os.getcwd()}'
        )

    # create hail service accounts
    if not do_not_create_hail_service_accounts:
        for c in clouds:
            create_hail_accounts(dataset, cloud=c)

    if configure_hail_batch_project:
        setup_hail_batch_billing_project(clouds=clouds, project=dataset)

    if Cloud.GCP in clouds:
        # True if created, else False if it already existed
        created_gcp_project = gcp_create_project(project_id=_gcp_project)
        if created_gcp_project:
            gcp_assign_billing_account(_gcp_project)
            gcp_create_budget(_gcp_project, amount=budgets[Cloud.GCP])

    logging.info(f'Creating dataset "{dataset}" with GCP id {_gcp_project}.')

    create_sample_metadata_project(dataset, _gcp_project)

    create_stack(
        clouds=clouds,
        gcp_project=_gcp_project,
        add_as_seqr_dependency=add_as_seqr_dependency,
        dataset=dataset,
        create_release_buckets=create_release_buckets,
        load_hail_service_accounts=not do_not_create_hail_service_accounts,
    )

    if deploy_stack:
        env = {**os.environ, 'PULUMI_CONFIG_PASSPHRASE': get_pulumi_config_passphrase()}
        rc = subprocess.call(['pulumi', 'up', '--stack', 'production', '-y'], env=env)
        if rc != 0:
            raise ValueError(f'The stack {dataset} did not deploy correctly')

    if generate_service_account_key:
        generate_upload_account_json(dataset=dataset, gcp_project=_gcp_project)


def create_sample_metadata_project(dataset, _gcp_project):
    """Create the metamist project"""
    papi = ProjectApi()
    projects = papi.get_all_projects()
    already_created = any(p.get('dataset') == dataset for p in projects)
    if not already_created:
        logging.info('Setting up sample-metadata project')
        papi.create_project(
            name=dataset,
            dataset=dataset,
            gcp_id=_gcp_project,
            create_test_project=True,
        )


def gcp_create_project(
    project_id, organisation_id=ORGANIZATION_ID, return_if_already_exists=True
):
    """Call subprocess.check_output to create project under an organisation"""
    # check if exists
    existence_command = ['gcloud', 'projects', 'list', '--filter', project_id]
    existing_projects_output = (
        subprocess.check_output(existence_command, stderr=subprocess.STDOUT)
        .decode()
        .split('\n')
    )
    existing_projects_output = [line for line in existing_projects_output if line]
    if len(existing_projects_output) == 2:
        # exists
        if not return_if_already_exists:
            raise ValueError(f'Project {project_id} already exists')

        logging.info('GCP project already exists, not creating')
        return False

    logging.info(f'Creating GCP project {project_id}')

    command = [
        'gcloud',
        'projects',
        'create',
        project_id,
        '--organization',
        organisation_id,
    ]
    subprocess.check_output(command)
    return True


def az_set_subscription(az_subscription_id):
    """Given an azure subscription id set the subscription"""
    command = ['az', 'account', 'set', '--subscription', az_subscription_id]
    subprocess.check_output(command)
    logging.info(f'Set az cli to subscription {az_subscription_id}.')


def az_create_resource_group(_azure_project, az_subscription_id):
    """Create a resoure group withing the subscription"""
    az_set_subscription(az_subscription_id)
    command = [
        'az',
        'group',
        'create',
        '--name',
        _azure_project,
        '--location',
        AZURE_LOCATION,
    ]
    out = subprocess.check_output(command)
    logging.info(f'Created resource group {_azure_project} in {az_subscription_id}.')
    return out


def gcp_assign_billing_account(project_id, billing_account_id=BILLING_ACCOUNT_ID):
    """
    Assign a billing account to a GCP project
    """
    logging.info('Assigning billing account')

    command = [
        'gcloud',
        'beta',
        'billing',
        'projects',
        'link',
        project_id,
        '--billing-account',
        billing_account_id,
    ]
    subprocess.check_output(command)
    logging.info(f'Assigned a billing account to {project_id}.')


def gcp_create_budget(project_id: str, amount=100):
    """
    Create a monthly budget for the project_id
    """
    budget = Budget(
        display_name=project_id,
        budget_filter=Filter(
            projects=[f'projects/{project_id}'],
            calendar_period=CalendarPeriod.MONTH,
        ),
        amount=BudgetAmount(
            specified_amount=Money(currency_code='AUD', units=amount, nanos=0),
        ),
        threshold_rules=[
            ThresholdRule(threshold_percent=0.5),
            ThresholdRule(threshold_percent=0.9),
            ThresholdRule(threshold_percent=1.0),
        ],
        notifications_rule=NotificationsRule(
            pubsub_topic=f'projects/{BILLING_PROJECT_ID}/topics/budget-notifications',
            schema_version='1.0',
        ),
    )
    logging.info(f'Creating budget (amount={budget}) for {project_id}')
    try:
        resp = BudgetServiceClient(
            client_options=ClientOptions(quota_project_id=BILLING_PROJECT_ID)
        ).create_budget(budget=budget, parent=f'billingAccounts/{BILLING_ACCOUNT_ID}')
        logging.info(f'Budget created successfully, {resp}')
    except GoogleAPICallError as rpc_error:
        logging.error(rpc_error)
        raise

    logging.info(f'Created budget for {project_id}')

    return True


def get_hail_service_accounts(dataset: str, clouds: list[Cloud]):
    """Get hail service accounts from kubectl"""
    setup_cluster = {
        Cloud.AZURE: [
            'az',
            'aks',
            'get-credentials',
            '--name',
            'vdc',
            '--resource-group',
            'hail',
            f'--subscription',
            AZURE_HAIL_SUBSCRIPTION,
        ],
        Cloud.GCP: [
            'gcloud',
            f'--project={GCP_HAIL_PROJECT}',
            'container',
            'clusters',
            'get-credentials',
            'vdc',
            '--zone=australia-southeast1-b',
        ],
    }

    identifier_by_cloud = {
        Cloud.GCP: 'client_email',
        Cloud.AZURE: 'objectId',
    }

    hail_client_emails_by_cloud_then_level = defaultdict(dict)
    for cloud in clouds:
        cloud_identifier = identifier_by_cloud[cloud]
        subprocess.check_output(setup_cluster[cloud])

        for access_level in ('test', 'standard', 'full'):
            hail_token = subprocess.check_output(
                _kubectl_hail_token_command(dataset, access_level), shell=True
            ).decode()
            # The hail_token from kubectl looks like: { "key.json": "<service-account-json-string>" }
            sa_key = json.loads(json.loads(hail_token)['key.json'])
            hail_client_emails_by_cloud_then_level[cloud][access_level] = sa_key[
                cloud_identifier
            ]

    return hail_client_emails_by_cloud_then_level


def setup_hail_batch_billing_project(project: str, clouds: list[Cloud]):
    """
    (If required) Create a Hail batch billing project
    (If required) Add standard + aggregator users to batch billing project

    Subsequent runs of this method produces no action.
    """

    for cloud in clouds:
        hail_auth_token = _get_hail_auth_token(cloud=cloud)

        # determine list of users we want in batch billing_project
        usernames = set(_get_standard_hail_account_names(project))
        if BILLING_AGGREGATOR_USERNAME:
            usernames.add(BILLING_AGGREGATOR_USERNAME)

        url = HAIL_GET_BILLING_PROJECT_PATH.format(
            hail_batch_url=HAIL_BATCH_URL.get(cloud), billing_project=project
        )
        resp = requests.get(
            url, headers={'Authorization': f'Bearer {hail_auth_token}'}, timeout=TIMEOUT
        )

        usernames_already_in_project = set()
        # it throws a 403 user error, but may as well check for 404 too
        if resp.status_code in (403, 404):
            _hail_batch_create_billing_project(
                cloud=cloud, project=project, hail_auth_token=hail_auth_token
            )
        else:
            # check for any other batch errors
            resp.raise_for_status()
            usernames_already_in_project = set(resp.json()['users'])

        for username in usernames - usernames_already_in_project:
            _hail_batch_add_user_to_billing_project(
                cloud=cloud,
                billing_project=project,
                username=username,
                hail_auth_token=hail_auth_token,
            )


def _hail_batch_create_billing_project(cloud, project, hail_auth_token):
    url = HAIL_CREATE_BILLING_PROJECT_PATH.format(
        hail_batch_url=HAIL_BATCH_URL.get(cloud), billing_project=project
    )
    resp = requests.post(
        url, headers={'Authorization': f'Bearer {hail_auth_token}'}, timeout=TIMEOUT
    )
    resp.raise_for_status()


def _hail_batch_add_user_to_billing_project(
    cloud, billing_project, username, hail_auth_token
):
    url = HAIL_ADD_USER_TO_BILLING_PROJECT_PATH.format(
        hail_batch_url=HAIL_BATCH_URL.get(cloud),
        billing_project=billing_project,
        user=username,
    )
    resp = requests.post(
        url, headers={'Authorization': f'Bearer {hail_auth_token}'}, timeout=TIMEOUT
    )
    resp.raise_for_status()


def _kubectl_hail_token_command(project, access_level: str):
    return f"kubectl get secret {project}-{access_level}-gsa-key -o json | jq '.data | map_values(@base64d)'"


def _check_if_hail_account_exists(username, hail_auth_token, cloud: Cloud = Cloud.GCP):
    url = HAIL_GET_USER_PATH.format(
        username=username, hail_auth_url=HAIL_AUTH_URL.get(cloud)
    )
    resp = requests.get(
        url, headers={'Authorization': f'Bearer {hail_auth_token}'}, timeout=TIMEOUT
    )

    if resp.status_code == 404:
        return False

    resp.raise_for_status()

    return resp.ok


def _check_if_hail_account_is_active(username, hail_auth_token, cloud: Cloud) -> bool:
    """Check if a hail account is active"""

    url = HAIL_GET_USER_PATH.format(
        username=username, hail_auth_url=HAIL_AUTH_URL.get(cloud)
    )
    resp = requests.get(
        url, headers={'Authorization': f'Bearer {hail_auth_token}'}, timeout=TIMEOUT
    )

    if not resp.ok:
        return False

    j = resp.json()
    return j['state'] == 'active'


def _create_hail_service_account(username, hail_auth_token, cloud: Cloud = Cloud.GCP):
    url = HAIL_CREATE_USER_PATH.format(
        username=username, hail_auth_url=HAIL_AUTH_URL.get(cloud)
    )
    post_resp = requests.post(
        url=url,
        headers={'Authorization': f'Bearer {hail_auth_token}'},
        data=json.dumps(
            {
                'user': username,
                'login_id': None,
                'is_developer': False,
                'is_service_account': True,
            }
        ),
        timeout=TIMEOUT,
    )
    post_resp.raise_for_status()


def _get_standard_hail_account_names(dataset):
    username_suffixes = ['-test', '-standard', '-full']
    return [dataset + suffix for suffix in username_suffixes]


def _get_hail_auth_token(cloud: Cloud):
    with open(os.path.expanduser('~/.hail/tokens.json'), encoding='utf-8') as f:
        return json.load(f)[cloud.value]


def create_hail_accounts(dataset, cloud: Cloud = Cloud.GCP):
    """
    Create 3 service accounts ${ds}-{test,standard,full} in Hail Batch
    """
    # Based on: https://github.com/hail-is/hail/pull/11249
    hail_auth_token = _get_hail_auth_token(cloud)
    potential_usernames = _get_standard_hail_account_names(dataset)
    # check if it exists
    usernames = []
    for username in potential_usernames:
        if not _check_if_hail_account_exists(
            username, hail_auth_token=hail_auth_token, cloud=cloud
        ):
            usernames.append(username)

    for username in usernames:
        _create_hail_service_account(
            username, hail_auth_token=hail_auth_token, cloud=cloud
        )

    # wait for all to be done

    for username in potential_usernames:
        counter = 0
        while counter < 10:
            if _check_if_hail_account_is_active(username, hail_auth_token, cloud):
                logging.info(f'{cloud} Hail account {username} is active')
                break

            counter += 1
            time.sleep(5.0)


def create_stack(
    clouds: list[Cloud],
    gcp_project: str,
    dataset: str,
    add_as_seqr_dependency: bool,
    create_release_buckets: bool,
    load_hail_service_accounts: bool,
):
    """
    Generate Pulumi.{dataset}.yaml pulumi stack file, with required params
    """

    dataset_config = {
        'archive_age': 90,
        'enable_release': create_release_buckets,
        'deploy_locations': [c.value for c in clouds],
    }

    if gcp_project:
        dataset_config['gcp'] = {'project': gcp_project}

    if Cloud.AZURE in clouds:
        dataset_config['azure'] = {}

    if load_hail_service_accounts:
        hail_client_emails_by_level = get_hail_service_accounts(
            dataset=dataset, clouds=clouds
        )

        for cloud, data in hail_client_emails_by_level.items():
            dataset_config[cloud.value].update(
                {
                    f'hail_service_account_{access_level}': account
                    for access_level, account in data.items()
                }
            )

    with open('production.yaml', 'r+', encoding='utf-8') as fp:
        production_config = yaml.safe_load(fp)
        if dataset in production_config:
            existing_config = production_config[dataset]
            if existing_config != dataset_config:

                keys_to_check = set(
                    list(existing_config.keys()) + list(dataset_config.keys())
                )
                mismatched_keys = [
                    k
                    for k in keys_to_check
                    if existing_config.get(k) != dataset_config.get(k)
                ]

                if not mismatched_keys:
                    return

                warning = ' | '.join(
                    f'{k}: {existing_config.get(k)} != {dataset_config.get(k)}'
                    for k in mismatched_keys
                )

                message = (
                    f'The pulumi stack file already exists and is not identical ({warning}), '
                    f'do you want to recreate it?'
                )
                if not click.confirm(message):
                    return

        if add_as_seqr_dependency:
            depends_on = production_config['seqr'].get('depends_on')
            if dataset in depends_on:
                # it's already there!
                return
            production_config['seqr']['depends_on'] = [*depends_on, dataset]

        production_config[dataset] = dataset_config

        fp.seek(0)
        yaml.dump(production_config, fp)


def generate_upload_account_json(dataset, gcp_project):
    """
    Generate access JSON for main-upload service account
    """
    service_account_fn = os.path.join(os.getcwd(), f'{dataset}-sa-upload.json')
    subprocess.check_output(
        [
            *('gcloud', 'iam', 'service-accounts', 'keys', 'create'),
            service_account_fn,
            f'--iam-account=main-upload@{gcp_project}.iam.gserviceaccount.com',
        ]
    )
    logging.info(f'Generated service account: {service_account_fn}')


def add_dataset_to_tokens(dataset: str):
    """
    Add dataset to the tokens/repository-map.json to
    make permission related caches populate correctly
    """
    with open('../tokens/repository-map.json', 'r', encoding='utf-8') as f:
        d = json.load(f)

    if dataset in d:
        # It's already there!
        return False
    d[dataset] = []

    with open('../tokens/repository-map.json', 'w+', encoding='utf-8') as f:
        json.dump(d, f, indent=4, sort_keys=True)
        f.write('\n')  # Make the inter happy.

    return True


if __name__ == '__main__':
    if os.getenv('DEBUG'):
        import debugpy

        debugpy.listen(('localhost', 5678))
        print('debugpy is listening, attach by pressing F5 or ???')

        debugpy.wait_for_client()
        print('Attached to debugpy!')

    main()  # pylint: disable=no-value-for-parameter
