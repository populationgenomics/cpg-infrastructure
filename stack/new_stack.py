#!/usr/bin/env python3

"""
Create GCP project + stack file for Pulumi

requirements:
    - click pyyaml sample-metadata google-cloud-billing-budgets
    - kubectl needs to be installed in the environment
        (due to subprocess call)

Example usage:

    cd stack
    DATASET="my-dataset"
    python new_stack.py \
        --dataset $DATASET \
        --perform-all --no-commit \
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
from stack_utils import get_pulumi_config_passphrase
from sample_metadata.apis import ProjectApi


TRUTHY_VALUES = ('y', '1', 't')
DATASET_REGEX = r'^[a-z][a-z0-9-]{1,15}[a-z]$'
GCP_PROJECT_REGEX = r'^[a-z0-9-_]{6,30}$'

ORGANIZATION_ID = '648561325637'
BILLING_ACCOUNT_ID = '01D012-20A6A2-CBD343'
BILLING_PROJECT_ID = 'billing-admin-290403'
HAIL_PROJECT = 'hail-295901'

HAIL_AUTH_URL = 'https://auth.hail.populationgenomics.org.au'
HAIL_CREATE_USER_PATH = HAIL_AUTH_URL + '/api/v1alpha/users/{username}/create'
HAIL_GET_USER_PATH = HAIL_AUTH_URL + '/api/v1alpha/users/{username}'

logging.basicConfig(level=logging.INFO)


@click.command()
@click.option('--dataset')
@click.option('--gcp-project', required=False, help='If different to the dataset name')
@click.option('--budget', type=int, help='Monthly budget in whole AUD', default=100)
@click.option('--add-random-digits-to-gcp-id', required=False, is_flag=True)
@click.option('--create-release-buckets', required=False, is_flag=True)
@click.option(
    '--perform-all',
    required=False,
    is_flag=True,
    help='Set-up GCP project + billing, deploy stack and create SM project',
)
@click.option('--create-gcp-project', required=False, is_flag=True)
@click.option('--setup-gcp-billing', required=False, is_flag=True)
@click.option('--create-hail-service-accounts', required=False, is_flag=True)
@click.option('--create-pulumi-stack', required=False, is_flag=True)
@click.option('--add-to-seqr-stack', required=False, is_flag=True)
@click.option('--deploy-stack', required=False, is_flag=True, help='Runs `pulumi up`')
@click.option('--create-sample-metadata-project', required=False, is_flag=True)
@click.option('--generate-service-account-key', required=False, is_flag=True)
@click.option('--no-commit', required=False, is_flag=True)
def main(
    dataset: str,
    gcp_project: str = None,
    budget: int = 100,
    add_random_digits_to_gcp_id=False,
    create_release_buckets=False,
    perform_all=False,
    create_gcp_project=False,
    setup_gcp_billing=False,
    create_hail_service_accounts=False,
    create_pulumi_stack=False,
    add_to_seqr_stack=False,
    deploy_stack=False,
    create_sample_metadata_project=False,
    generate_service_account_key=False,
    no_commit=False,
):
    """Function that coordinates creating a project"""

    if perform_all:
        create_gcp_project = True
        setup_gcp_billing = True
        create_hail_service_accounts = True
        create_pulumi_stack = True
        create_sample_metadata_project = True

    dataset = dataset.lower()
    _gcp_project = gcp_project
    if not gcp_project:
        suffix = ''
        if add_random_digits_to_gcp_id:
            suffix = '-' + str(random.randint(100000, 999999))
        _gcp_project = dataset + suffix
    _gcp_project = _gcp_project.lower()

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

    logging.info(f'Creating dataset "{dataset}" with GCP id {_gcp_project}.')

    if create_hail_service_accounts:
        create_hail_accounts(dataset)

    if create_gcp_project:
        # True if created, else False if it already existed
        created_gcp_project = create_project(project_id=_gcp_project)
        if created_gcp_project:
            assign_billing_account(_gcp_project)

            if setup_gcp_billing:
                create_budget(_gcp_project, amount=budget)

    if create_sample_metadata_project:
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

    pulumi_config_fn = f'Pulumi.{dataset}.yaml'
    if create_pulumi_stack:
        create_stack(
            pulumi_config_fn=pulumi_config_fn,
            gcp_project=_gcp_project,
            add_to_seqr_stack=add_to_seqr_stack,
            dataset=dataset,
            create_release_buckets=create_release_buckets,
            should_commit=not no_commit,
        )

    if not os.path.exists(pulumi_config_fn):
        raise ValueError(f'Expected to find {pulumi_config_fn}, but it did not exist')

    if deploy_stack:
        env = {**os.environ, 'PULUMI_CONFIG_PASSPHRASE': get_pulumi_config_passphrase()}
        rc = subprocess.call(['pulumi', 'up', '-y'], env=env)
        if rc != 0:
            raise ValueError(f'The stack {dataset} did not deploy correctly')

    if generate_service_account_key:
        generate_upload_account_json(dataset=dataset, gcp_project=_gcp_project)


def create_project(
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


def assign_billing_account(project_id, billing_account_id=BILLING_ACCOUNT_ID):
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


def create_budget(project_id: str, amount=100):
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


def get_hail_service_accounts(dataset: str):
    """Get hail service accounts from kubectl"""
    subprocess.check_output(
        [
            'gcloud',
            f'--project={HAIL_PROJECT}',
            'container',
            'clusters',
            'get-credentials',
            'vdc',
            '--zone=australia-southeast1-b',
        ]
    )
    hail_client_emails_by_level = {}
    for access_level in ('test', 'standard', 'full'):
        hail_token = subprocess.check_output(
            _kubectl_hail_token_command(dataset, access_level), shell=True
        ).decode()
        # The hail_token from kubectl looks like: { "key.json": "<service-account-json-string>" }
        sa_key = json.loads(json.loads(hail_token)['key.json'])
        hail_client_emails_by_level[access_level] = sa_key['client_email']

    return hail_client_emails_by_level


def _kubectl_hail_token_command(project, access_level: str):
    return f"kubectl get secret {project}-{access_level}-gsa-key -o json | jq '.data | map_values(@base64d)'"


def _check_if_hail_account_exists(username, hail_auth_token):
    url = HAIL_GET_USER_PATH.format(username=username)
    resp = requests.get(
        url,
        headers={'Authorization': f'Bearer {hail_auth_token}'},
    )

    if resp.status_code == 404:
        return False

    resp.raise_for_status()

    return resp.ok


def _check_if_hail_account_is_active(username, hail_auth_token) -> bool:
    """Check if a hail account is active"""

    url = HAIL_GET_USER_PATH.format(username=username)
    resp = requests.get(
        url,
        headers={'Authorization': f'Bearer {hail_auth_token}'},
    )

    if not resp.ok:
        return False

    j = resp.json()
    return j['state'] == 'active'


def _create_hail_service_account(username, hail_auth_token):
    url = HAIL_CREATE_USER_PATH.format(username=username)
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
    )
    post_resp.raise_for_status()


def create_hail_accounts(dataset):
    """
    Create 3 service accounts ${ds}-{test,standard,full} in Hail Batch
    """
    # Based on: https://github.com/hail-is/hail/pull/11249
    with open(os.path.expanduser('~/.hail/tokens.json'), encoding='utf-8') as f:
        hail_auth_token = json.load(f)['default']

    username_suffixes = ['-test', '-standard', '-full']
    potential_usernames = [dataset + suffix for suffix in username_suffixes]

    # check if it exists
    usernames = []
    for username in potential_usernames:
        if not _check_if_hail_account_exists(username, hail_auth_token=hail_auth_token):
            usernames.append(username)

    for username in usernames:
        _create_hail_service_account(username, hail_auth_token=hail_auth_token)

    # wait for all to be done

    for username in potential_usernames:
        counter = 0
        while counter < 10:
            if _check_if_hail_account_is_active(username, hail_auth_token):
                logging.info(f'Hail account {username} is active')
                break

            counter += 1
            time.sleep(5.0)


def create_stack(
    pulumi_config_fn: str,
    gcp_project: str,
    dataset: str,
    add_to_seqr_stack: bool,
    create_release_buckets: bool,
    should_commit=True,
):
    """
    Generate Pulumi.{dataset}.yaml pulumi stack file, with required params
    """
    if os.path.exists(pulumi_config_fn):
        if not click.confirm(
            'The pulumi stack file already existed, do you want to recreate it?'
        ):
            return

    branch_name = f'add-{dataset}-stack'
    if should_commit:
        current_branch = subprocess.check_output(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
        ).decode()
        if current_branch != 'main':
            should_continue = click.confirm(
                f'Expected branch to be "main", got "{current_branch}". '
                'Do you want to continue (and branch from this branch) (y/n)? '
            )
            if not should_continue:
                raise SystemExit
        elif current_branch != branch_name:
            try:
                subprocess.check_output(['git', 'checkout', '-b', branch_name])
            except subprocess.CalledProcessError:
                logging.error(
                    f'There was an issue checking out the new branch {branch_name}'
                )
                raise

    hail_client_emails_by_level = get_hail_service_accounts(dataset=dataset)

    formed_hail_config = {
        f'datasets:hail_service_account_{access_level}': value
        for access_level, value in hail_client_emails_by_level.items()
    }
    pulumi_stack = {
        'config': {
            'datasets:archive_age': '90',
            'datasets:customer_id': 'C010ys3gt',
            'datasets:enable_release': create_release_buckets,
            'gcp:billing_project': gcp_project,
            'gcp:project': gcp_project,
            'gcp:user_project_override': 'true',
            **formed_hail_config,
        },
    }

    with open(pulumi_config_fn, 'w+', encoding='utf-8') as fp:
        logging.info(f'Writing to {pulumi_config_fn}')
        yaml.dump(pulumi_stack, fp, default_flow_style=False)

    files_to_add = [pulumi_config_fn]

    if add_to_seqr_stack:
        add_dataset_to_seqr_depends_on(dataset)
        files_to_add.append('Pulumi.seqr.yaml')

    add_dataset_to_tokens(dataset)
    files_to_add.append('../tokens/repository-map.json')

    # Creating the stack sets the config passphrase encryption salt.
    env = {**os.environ, 'PULUMI_CONFIG_PASSPHRASE': get_pulumi_config_passphrase()}
    subprocess.check_output(['pulumi', 'stack', 'select', '--create', dataset], env=env)

    if should_commit:
        logging.info('Preparing git commit')

        subprocess.check_output(['git', 'add', *files_to_add])

        default_commit_message = f'Adds {dataset} dataset'
        commit_message = str(
            input(f'Commit message (default="{default_commit_message}"): ')
        )
        subprocess.check_output(
            ['git', 'commit', '-m', commit_message or default_commit_message]
        )
        logging.info(
            f'Created stack, you can push this with:\n\n'
            f'\tgit push --set-upstream origin {branch_name}'
        )
    else:
        logging.info(
            f"""
Created stack {dataset}, you can commit and push this with:

    git checkout -b add-{dataset}-stack
    git add {' '.join(files_to_add)}
    git push --set-upstream origin {branch_name}
"""
        )


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


def add_dataset_to_seqr_depends_on(dataset: str):
    """
    Add dataset to depends_on in seqr stack
    """
    with open('Pulumi.seqr.yaml', 'r+', encoding='utf-8') as f:
        d = yaml.safe_load(f)
        config = d['config']
        depends_on = json.loads(config['datasets:depends_on'])
        if dataset in depends_on:
            # it's already there!
            return
        config['datasets:depends_on'] = json.dumps([*depends_on, dataset])
        # go back to the start for writing to disk
        f.seek(0)
        yaml.dump(d, f, default_flow_style=False)


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
    d[dataset] = ['sample-metadata']

    with open('../tokens/repository-map.json', 'w+', encoding='utf-8') as f:
        json.dump(d, f, indent=4, sort_keys=True)
        f.write('\n')  # Make the inter happy.

    return True


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
