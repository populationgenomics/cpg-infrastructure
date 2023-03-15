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
import contextlib
import os
from base64 import b64encode
from functools import cached_property

import pulumi
import pulumi_gcp as gcp
from cpg_utils.cloud import read_secret

from cpg_infra.config import CPGInfrastructureConfig

PATH_TO_AGGREGATE_SOURCE_CODE = os.path.join(os.path.dirname(__file__), 'aggregate')
PATH_TO_MONTHLY_AGGREGATE_SOURCE_CODE = os.path.join(
    os.path.dirname(__file__), 'monthly_aggregate'
)


class BillingAggregator:
    """Billing aggregator Infrastructure (as code) for Pulumi"""

    def __init__(self, config: CPGInfrastructureConfig):
        self.config = config

    def main(self):
        """
        Setup the billing aggregator cloud functions,
        these are designed to only work on GCP, so no abstraction
        """
        if not self.config.billing.aggregator:
            print('Skipping billing aggregator config was not present')
            return

        self.setup_aggregator_functions()
        self.setup_monthly_export()

    @cached_property
    def functions_service(self):
        return gcp.projects.Service(
            'billing-aggregator-cloudfunctions-service',
            service='cloudfunctions.googleapis.com',
            project=self.config.billing.gcp.project_id,
            disable_on_destroy=False,
        )

    @cached_property
    def pubsub_service(self):
        return gcp.projects.Service(
            'billing-aggregator-pubsub-service',
            service='pubsub.googleapis.com',
            project=self.config.billing.gcp.project_id,
            disable_on_destroy=False,
        )

    @cached_property
    def scheduler_service(self):
        return gcp.projects.Service(
            'billing-aggregator-cloudscheduler-service',
            service='cloudscheduler.googleapis.com',
            project=self.config.billing.gcp.project_id,
            disable_on_destroy=False,
        )

    @cached_property
    def build_service(self):
        return gcp.projects.Service(
            'billing-aggregator-cloudbuild-service',
            service='cloudbuild.googleapis.com',
            project=self.config.billing.gcp.project_id,
            disable_on_destroy=False,
        )

    @cached_property
    def source_bucket(self):
        """
        We will store the source code to the Cloud Function
        in a Google Cloud Storage bucket.
        """
        return gcp.storage.Bucket(
            f'billing-aggregator-source-bucket',
            name=f'{self.config.dataset_storage_prefix}aggregator-source-bucket',
            location=self.config.gcp.region,
            project=self.config.billing.gcp.project_id,
            uniform_bucket_level_access=True,
        )

    @cached_property
    def slack_channel(self):
        """
        Create a Slack notification channel for all functions
        Use cli command below to retrieve the required 'labels'
        $ gcloud beta monitoring channel-descriptors describe slack
        """
        return gcp.monitoring.NotificationChannel(
            f'billing-aggregator-slack-notification-channel',
            display_name=f'Billing Aggregator Slack Notification Channel',
            type='slack',
            labels={'channel_name': self.config.billing.aggregator.slack_channel},
            sensitive_labels=gcp.monitoring.NotificationChannelSensitiveLabelsArgs(
                auth_token=read_secret(
                    project_id=self.config.billing.gcp.project_id,
                    secret_name=self.config.billing.aggregator.slack_token_secret_name,
                    fail_gracefully=False,
                ),
            ),
            description='Slack notification channel for all cost aggregator functions',
            project=self.config.billing.gcp.project_id,
        )

    def setup_monthly_export(self):
        if not (
            self.config.billing.aggregator.billing_sheet_id
            and self.config.billing.aggregator.monthly_summary_table
        ):
            print(
                'Skipping monthly export as billing_sheet_id / '
                'monthly_summary_table were not set'
            )
            return

        # The Cloud Function source code itself needs to be zipped up into an
        # archive, which we create using the pulumi.AssetArchive primitive.
        archive = archive_folder(PATH_TO_MONTHLY_AGGREGATE_SOURCE_CODE)
        # Create the single Cloud Storage object, which contains the source code
        source_archive_object = gcp.storage.BucketObject(
            'billing-monthly-aggregator-source-code',
            # updating the source archive object does not trigger the cloud function
            # to actually updating the source because it's based on the name,
            # allow Pulumi to create a new name each time it gets updated
            bucket=self.source_bucket.name,
            source=archive,
            opts=pulumi.ResourceOptions(replace_on_changes=['*']),
        )

        pubsub = gcp.pubsub.Topic(
            f'billing-monthly-aggregator-topic',
            project=self.config.billing.gcp.project_id,
            opts=pulumi.ResourceOptions(depends_on=[self.pubsub_service]),
        )

        # Create a cron job to run the aggregator function on some interval
        _ = gcp.cloudscheduler.Job(
            f'billing-monthly-aggregator-scheduler-job',
            pubsub_target=gcp.cloudscheduler.JobPubsubTargetArgs(
                topic_name=pubsub.id,
                data=b64encode_str('Run the functions'),
            ),
            # 3rd day of the month
            schedule=f'0 0 3 * *',
            project=self.config.billing.gcp.project_id,
            region=self.config.gcp.region,
            time_zone='Australia/Sydney',
            opts=pulumi.ResourceOptions(depends_on=[self.scheduler_service]),
        )

        _ = self.create_cloud_function(
            resource_name=f'billing-monthly-aggregator-function',
            name='monthly-aggregator',
            service_account=self.config.billing.coordinator_machine_account,
            pubsub_topic=pubsub,
            source_archive_object=source_archive_object,
            notification_channel=self.slack_channel,
            env={
                # 'SETUP_GCP_LOGGING': 'true',
                'OUTPUT_BILLING_SHEET': self.config.billing.aggregator.billing_sheet_id,
                'BQ_MONTHLY_SUMMARY_TABLE': self.config.billing.aggregator.monthly_summary_table,
            },
        )

    def setup_aggregator_functions(self):
        """Setup hourly aggregator functions"""
        if not 0 < self.config.billing.aggregator.interval_hours <= 24:
            raise ValueError(
                f'Invalid aggregator interval, {self.config.billing.aggregator.interval_hours} '
                f'hours (0, 24]'
            )

        if 24 % self.config.billing.aggregator.interval_hours != 0:
            print(
                f'The aggregator interval ({self.config.billing.aggregator.interval_hours}hrs) '
                f'does not cleanly fit into 24 hours, this means there might be '
                f'two runs within the interval period'
            )

        # The Cloud Function source code itself needs to be zipped up into an
        # archive, which we create using the pulumi.AssetArchive primitive.
        archive = archive_folder(PATH_TO_AGGREGATE_SOURCE_CODE)

        # Create the single Cloud Storage object, which contains the source code
        source_archive_object = gcp.storage.BucketObject(
            'billing-aggregator-source-code',
            # updating the source archive object does not trigger the cloud function
            # to actually updating the source because it's based on the name,
            # allow Pulumi to create a new name each time it gets updated
            # name=f'aggregator-source-code.zip',
            bucket=self.source_bucket.name,
            source=archive,
            opts=pulumi.ResourceOptions(replace_on_changes=['*']),
        )

        # Create one pubsub to be triggered by the cloud scheduler
        pubsub = gcp.pubsub.Topic(
            f'billing-aggregator-topic',
            project=self.config.billing.gcp.project_id,
            opts=pulumi.ResourceOptions(depends_on=[self.pubsub_service]),
        )

        # Create a cron job to run the aggregator function on some interval
        _ = gcp.cloudscheduler.Job(
            f'billing-aggregator-scheduler-job',
            pubsub_target=gcp.cloudscheduler.JobPubsubTargetArgs(
                topic_name=pubsub.id,
                data=b64encode_str('Run the functions'),
            ),
            schedule=f'0 */{self.config.billing.aggregator.interval_hours} * * *',
            project=self.config.billing.gcp.project_id,
            region=self.config.gcp.region,
            time_zone='Australia/Sydney',
            opts=pulumi.ResourceOptions(depends_on=[self.scheduler_service]),
        )

        for function in self.config.billing.aggregator.functions:
            memory = '512M'
            if function in ('hail', 'seqr'):
                memory = '2048M'
            # Create the function, the trigger and subscription.
            _ = self.create_cloud_function(
                resource_name=f'billing-aggregator-{function}-billing-function',
                name=function,
                source_file=f'{function}.py',
                service_account=self.config.billing.coordinator_machine_account,
                pubsub_topic=pubsub,
                source_archive_object=source_archive_object,
                notification_channel=self.slack_channel,
                memory=memory,
                env={
                    # 'SETUP_GCP_LOGGING': 'true',
                    'GCP_AGGREGATE_DEST_TABLE': self.config.billing.aggregator.destination_bq_table,
                    'GCP_BILLING_SOURCE_TABLE': self.config.billing.aggregator.source_bq_table,
                    # cover at least the previous period as well
                    'DEFAULT_INTERVAL_HOURS': self.config.billing.aggregator.interval_hours
                    * 2,
                    'BILLING_PROJECT_ID': self.config.billing.gcp.project_id,
                },
            )

    # monthly billing aggregator

    def create_cloud_function(
        self,
        resource_name: str,
        name: str,
        service_account: str,
        pubsub_topic: gcp.pubsub.Topic,
        source_archive_object: gcp.storage.BucketObject,
        notification_channel: gcp.monitoring.NotificationChannel,
        env: dict,
        source_file: str | None = None,
        memory: str='512M'
    ):
        """
        Create a single Cloud Function. Include the pubsub trigger and event alerts
        """

        # Trigger for the function, subscribe to the pubusub topic
        trigger = gcp.cloudfunctionsv2.FunctionEventTriggerArgs(
            event_type='google.cloud.pubsub.topic.v1.messagePublished',
            trigger_region='australia-southeast1',
            pubsub_topic=pubsub_topic.id,
            retry_policy='RETRY_POLICY_DO_NOT_RETRY',
        )

        # Create the Cloud Function

        build_environment_variables = {}
        if source_file:
            build_environment_variables['GOOGLE_FUNCTION_SOURCE'] = source_file

        fxn = gcp.cloudfunctionsv2.Function(
            resource_name,
            event_trigger=trigger,
            build_config=gcp.cloudfunctionsv2.FunctionBuildConfigArgs(
                runtime='python311',
                entry_point='from_request',
                environment_variables=build_environment_variables,
                source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceArgs(
                    storage_source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceStorageSourceArgs(
                        bucket=self.source_bucket.name,
                        object=source_archive_object.name,
                    ),
                ),
            ),
            service_config=gcp.cloudfunctionsv2.FunctionServiceConfigArgs(
                max_instance_count=1,
                min_instance_count=0,
                available_memory=memory,
                timeout_seconds=540,
                environment_variables=env,
                ingress_settings='ALLOW_INTERNAL_ONLY',
                all_traffic_on_latest_revision=True,
                service_account_email=service_account,
            ),
            project=self.config.billing.gcp.project_id,
            location=self.config.gcp.region,
            opts=pulumi.ResourceOptions(
                depends_on=[self.functions_service, self.build_service]
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
        alert_policy = gcp.monitoring.AlertPolicy(
            f'billing-aggregator-{name}-alert',
            display_name=f'{name.capitalize()} Billing Function Error Alert',
            combiner='OR',
            notification_channels=[notification_channel.id],
            conditions=[alert_condition],
            alert_strategy=gcp.monitoring.AlertPolicyAlertStrategyArgs(
                notification_rate_limit=(
                    gcp.monitoring.AlertPolicyAlertStrategyNotificationRateLimitArgs(
                        period='300s'
                    )
                ),
            ),
            opts=pulumi.ResourceOptions(depends_on=[fxn]),
        )

        return fxn, trigger, alert_policy


def b64encode_str(s: str) -> str:
    return b64encode(s.encode('utf-8')).decode('utf-8')


def archive_folder(path: str) -> pulumi.AssetArchive:
    assets = {}
    allowed_extensions = {'.py', '.txt', '.json'}

    # python 3.11 thing, but allows you to temporarily change directory
    # into the path we're archiving, so we're not archiving the directory,
    # but just the code files. Otherwise the deploy fails.
    with contextlib.chdir(path):
        for filename in os.listdir('.'):
            if not any(filename.endswith(ext) for ext in allowed_extensions):
                # print(f'Skipping {filename} for invalid extension')
                continue

            with open(filename, encoding='utf-8') as file:
                # do it this way to stop any issues with changing paths
                assets[filename] = pulumi.StringAsset(file.read())
        return pulumi.AssetArchive(assets)
