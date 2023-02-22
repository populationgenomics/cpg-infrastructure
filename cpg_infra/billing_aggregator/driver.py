# !/usr/bin/env python
# Disable rule for that module-level exports be ALL_CAPS, for legibility.
# pylint: disable=C0103,missing-function-docstring,W0613
"""
Python Pulumi program for creating Aggregate Billing Function Stack.

Requires:
    - Service Account in the gcp project of deployment
        (see .github/workflows/deploy-aggregate.yml for details)
    - CPGConfiguration.billing.aggregator configugration

Creates the following:
    - Enable Cloud Function Service
    - Create a bucket for the function source code
    - Create bucket object for the function source code and put it in the bucket
    - Create a pubsub topic and cloud scheduler for the functions
    - Create a slack notification channel for all functions
    - Create a cloud function for each function

TODO:

    - action monthly billing function
"""

import os
from base64 import b64encode

import pulumi
import pulumi_gcp as gcp

from cpg_infra.config import CPGInfrastructureConfig

PATH_TO_SOURCE_CODE = os.path.join(os.path.dirname(__file__), 'aggregate')


def setup_billing_aggregator(config: CPGInfrastructureConfig):
    """
    Setup the billing aggregator cloud functions,
    these are designed to only work on GCP, so no abstraction
    """
    # return
    if not config.billing.aggregator:
        print('Skipping billing aggregator config was not present')
        return

    # File path to where the Cloud Function's source code is located.
    if not (0 < config.billing.aggregator.interval_hours <= 24):
        raise ValueError(
            f'Invalid aggregator interval, {config.billing.aggregator.interval_hours} '
            f'hours (0, 24]'
        )

    # Set environment variable to the correct project

    # Start by enabling cloud function services
    cloud_service = gcp.projects.Service(
        'cloudfunctions-service',
        service='cloudfunctions.googleapis.com',
        disable_on_destroy=False,
    )

    # We will store the source code to the Cloud Function
    # in a Google Cloud Storage bucket.
    function_bucket = gcp.storage.Bucket(
        f"billing-aggregator-source-bucket",
        name=f"{config.dataset_storage_prefix}aggregator-source-bucket",
        location=config.gcp.region,
        project=config.billing.gcp.project_id,
        uniform_bucket_level_access=True,
    )

    # The Cloud Function source code itself needs to be zipped up into an
    # archive, which we create using the pulumi.AssetArchive primitive.
    archive = archive_folder(PATH_TO_SOURCE_CODE)

    # Create the single Cloud Storage object, which contains all of the function's
    # source code. ('main.py' and 'requirements.txt'.)
    source_archive_object = gcp.storage.BucketObject(
        'billing-aggregator-source-code',
        name=f'aggregator-source-code.tar',
        bucket=function_bucket.name,
        source=archive,
    )

    # Create one pubsub to be triggered by the cloud scheduler
    pubsub = gcp.pubsub.Topic(
        f'billing-aggregator-topic', project=config.billing.gcp.project_id
    )

    # Create a cron job to run the function on some interval
    _ = gcp.cloudscheduler.Job(
        f'billing-aggregator-scheduler-job',
        pubsub_target=gcp.cloudscheduler.JobPubsubTargetArgs(
            topic_name=pubsub.id,
            data=b64encode_str('Run the functions'),
        ),
        schedule=f'0 */{config.billing.aggregator.interval_hours} * * *',
        project=config.billing.gcp.project_id,
        region=config.gcp.region,
        time_zone='Australia/Sydney',
        opts=pulumi.ResourceOptions(depends_on=[pubsub]),
    )

    # Create slack notification channel for all functions
    # Use cli command below to retrieve the required 'labels'
    # $ gcloud beta monitoring channel-descriptors describe slack
    slack_channel = gcp.monitoring.NotificationChannel(
        f'billing-aggregator-slack-notification-channel',
        display_name=f'Billing Aggregator Slack Notification Channel',
        type='slack',
        user_labels={'channel_name': config.billing.aggregator.slack_channel},
        sensitive_labels=gcp.monitoring.NotificationChannelSensitiveLabelsArgs(
            auth_token=config.billing.aggregator.slack_token_secret_name,
        ),
        description='Slack notification channel for all cost aggregator functions',
        project=config.billing.gcp.project_id,
    )

    for function in config.billing.aggregator.functions:
        # Create the function and it's corresponding pubsub and subscription.
        fxn, _, _ = create_cloud_function(
            name=function,
            config=config,
            service_account=config.billing.coordinator_machine_account,
            pubsub_topic=pubsub,
            cloud_service=cloud_service,
            function_bucket=function_bucket,
            source_archive_object=source_archive_object,
            notification_channel=slack_channel,
        )


def b64encode_str(s: str) -> str:
    return b64encode(s.encode('utf-8')).decode('utf-8')


def create_cloud_function(
    name: str,
    config: CPGInfrastructureConfig,
    service_account: str,
    pubsub_topic: gcp.pubsub.Topic,
    function_bucket: gcp.storage.Bucket,
    cloud_service: gcp.projects.Service,
    source_archive_object: gcp.storage.BucketObject,
    notification_channel: gcp.monitoring.NotificationChannel,
):
    """
    Create a single Cloud Function. Include the pubsub trigger and event alerts
    """

    # Trigger for the function, subscribe to the pubusub topic
    trigger = gcp.cloudfunctions.FunctionEventTriggerArgs(
        event_type='google.pubsub.topic.publish', resource=pubsub_topic.name
    )

    # Create the Cloud Function
    env = {
        'GCP_AGGREGATE_DEST_TABLE': config.billing.aggregator.destination_bq_table,
        'GCP_BILLING_SOURCE_TABLE': config.billing.aggregator.source_bq_table,
        'DEFAULT_INTERVAL_HOURS': config.billing.aggregator.interval_hours,
        'BILLING_PROJECT_ID': config.billing.gcp.project_id,
    }
    fxn = gcp.cloudfunctions.Function(
        f'{name}-billing-function',
        entry_point=f'{name}',
        runtime='python310',
        event_trigger=trigger,
        source_archive_bucket=function_bucket.name,
        source_archive_object=source_archive_object.name,
        project=config.billing.gcp.project_id,
        region=config.gcp.region,
        build_environment_variables=env,
        environment_variables=env,
        service_account_email=service_account,
        available_memory_mb=1024,
        timeout=540,  # MAX timeout
        opts=pulumi.ResourceOptions(
            depends_on=[
                pubsub_topic,
                cloud_service,
                function_bucket,
                source_archive_object,
            ]
        ),
    )

    # Slack notifications
    filter_string = fxn.name.apply(
        lambda fxn_name: f"""
            resource.type="cloud_function"
            AND resource.labels.function_name="{fxn_name}"
            AND severity >= WARNING
        """
    )

    # Create the Cloud Function's event alert
    alert_condition = gcp.monitoring.AlertPolicyConditionArgs(
        condition_matched_log=(
            gcp.monitoring.AlertPolicyConditionConditionMatchedLogArgs(
                filter=filter_string,
            )
        ),
        display_name='Function warning/error',
    )
    alert_rate = gcp.monitoring.AlertPolicyAlertStrategyArgs(
        notification_rate_limit=(
            gcp.monitoring.AlertPolicyAlertStrategyNotificationRateLimitArgs(
                period='300s'
            )
        ),
    )
    alert_policy = gcp.monitoring.AlertPolicy(
        f'{name}-billing-function-error-alert',
        display_name=f'{name.capitalize()} Billing Function Error Alert',
        combiner='OR',
        notification_channels=[notification_channel.id],
        conditions=[alert_condition],
        alert_strategy=alert_rate,
        opts=pulumi.ResourceOptions(depends_on=[fxn]),
    )

    return fxn, trigger, alert_policy


def archive_folder(path: str) -> pulumi.AssetArchive:
    assets = {}
    allowed_extensions = ['.py', '.txt']
    for file in os.listdir(path):
        location = os.path.join(path, file)
        if os.path.isdir(location) and not location.startswith('__'):
            assets[file] = pulumi.FileArchive(location)
        elif any(location.endswith(ext) for ext in allowed_extensions):
            assets[file] = pulumi.FileAsset(path=location)
        # skip any other files,

    return pulumi.AssetArchive(assets)
