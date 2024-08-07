"""A Cloud Function to process GCP billing budget notifications."""

import json
import logging
import os
import socket
from typing import Any

import flask
import functions_framework
import slack
from google.auth.exceptions import RefreshError, TransportError
from google.cloud import secretmanager
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from slack.errors import SlackApiError

PROJECT_ID = os.getenv('GCP_PROJECT')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL')
SLACK_TOKEN_SECRET_NAME = (
    f'projects/{PROJECT_ID}/secrets/slack-gcp-cost-control/versions/latest'
)

# Cache the Slack client.
secret_manager = secretmanager.SecretManagerServiceClient()
slack_token_response = secret_manager.access_secret_version(
    request={'name': SLACK_TOKEN_SECRET_NAME},
)
slack_token = slack_token_response.payload.data.decode('UTF-8')
slack_client = slack.WebClient(token=slack_token)


@functions_framework.http
def gcp_cost_control(request: flask.Request):
    """Main entry point for the Cloud Function."""

    # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/f7828705deaeb743828a531d5c25bc2cc6505a06/run/pubsub/main.py#L30-L45
    envelope = request.get_json()
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    pubsub_message_data = envelope["message"]['data']
    pubsub_budget_notification_data = json.loads(pubsub_message_data)

    logging.info(f'Received notification: {pubsub_budget_notification_data}')

    budget = pubsub_budget_notification_data['budgetAmount']
    cost = pubsub_budget_notification_data['costAmount']

    if cost <= budget:
        logging.info('Still under budget')
        return None

    # The budget alert name must correspond to the corresponding project ID.
    budget_project_id = pubsub_budget_notification_data['budgetDisplayName']

    billing = discovery.build('cloudbilling', 'v1', cache_discovery=False)
    projects = billing.projects()  # pylint: disable=no-member

    # If the billing is already disabled, there's nothing to do.
    if not is_billing_enabled(budget_project_id, projects):
        logging.info('Billing is already disabled')
        return None

    logging.info('Over budget (%f > %f), disabling billing', cost, budget)
    disable_billing_for_project(budget_project_id, projects)

    currency = pubsub_budget_notification_data['currencyCode']
    post_slack_message(
        f"*Warning:* disabled billing for GCP project '{budget_project_id}', "
        f'which is over budget ({cost} {currency} > {budget} {currency}).',
    )
    return None


def is_billing_enabled(project_id: str, projects: Any) -> bool:
    """Determine whether billing is enabled for a project.

    @param {string} project_id ID of project to check if billing is enabled
    @return {bool} Whether project has billing enabled or not
    """
    try:
        res = projects.getBillingInfo(name=f'projects/{project_id}').execute()
        return res['billingEnabled']
    except KeyError:
        return False
    except HttpError as e:
        logging.error(f'An HTTP error occurred: {e.resp.status} {e.content}')
    except (RefreshError, TransportError) as e:
        logging.error(f'An authentication or network error occurred: {e}')
    except socket.timeout:
        logging.error('The request timed out')

    return False


def disable_billing_for_project(project_id: str, projects: Any):
    """Disable billing for a project by removing its billing account.

    @param {string} project_id ID of project disable billing on
    """
    body = {'billingAccountName': ''}  # Disable billing
    try:
        res = projects.updateBillingInfo(
            name=f'projects/{project_id}',
            body=body,
        ).execute()
        logging.error(f'Billing disabled: {json.dumps(res)}')
    except HttpError as e:
        logging.error(f'An HTTP error occurred: {e.resp.status} {e.content}')
    except (RefreshError, TransportError) as e:
        logging.error(f'An authentication or network error occurred: {e}')
    except socket.timeout:
        logging.error('The request timed out')


def post_slack_message(text: str):
    """Posts the given text as message to Slack."""

    try:
        slack_client.api_call(
            'chat.postMessage',
            json={
                'channel': SLACK_CHANNEL,
                'text': text,
            },
        )
    except SlackApiError as err:
        logging.error(f'Error posting to Slack: {err}')
