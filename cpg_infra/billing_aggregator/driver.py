# !/usr/bin/env python
# Disable rule for that module-level exports be ALL_CAPS, for legibility.
# flake8: noqa: ERA001
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
from functools import cached_property

import pulumi
import pulumi_gcp as gcp

from cpg_infra.plugin import CpgInfrastructurePlugin
from cpg_infra.utils import archive_folder
from cpg_utils.cloud import read_secret

PATH_TO_AGGREGATE_SOURCE_CODE = os.path.join(os.path.dirname(__file__), 'aggregate')
PATH_TO_MONTHLY_AGGREGATE_SOURCE_CODE = os.path.join(
    os.path.dirname(__file__),
    'monthly_aggregate',
)
PATH_TO_UPDATE_BUDGET_SOURCE_CODE = os.path.join(
    os.path.dirname(__file__),
    'update_budget',
)


def get_file_content(filename: str) -> str:
    """Read content of the file"""
    with open(filename, encoding='utf-8') as file:
        return file.read()


class BillingAggregator(CpgInfrastructurePlugin):
    """Billing aggregator Infrastructure (as code) for Pulumi"""

    def main(self):
        """
        Setup the billing aggregator cloud functions,
        these are designed to only work on GCP, so no abstraction
        """
        if not self.config.billing or not self.config.billing.aggregator:
            print('Skipping billing aggregator config was not present')
            return

        self.setup_aggregator_functions()
        self.setup_monthly_export()
        self.setup_update_budget()
        # setup BQ objects
        _ = self.aggregate_table
        self.setup_materialized_views()

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
            'billing-aggregator-source-bucket',
            name=f'{self.config.gcp.dataset_storage_prefix}aggregator-source-bucket',
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
            'billing-aggregator-slack-notification-channel',
            display_name='Billing Aggregator Slack Notification Channel',
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
                'monthly_summary_table were not set',
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
            'billing-monthly-aggregator-topic',
            project=self.config.billing.gcp.project_id,
            opts=pulumi.ResourceOptions(depends_on=[self.pubsub_service]),
        )

        # Create a cron job to run the aggregator function on some interval
        _ = gcp.cloudscheduler.Job(
            'billing-monthly-aggregator-scheduler-job',
            pubsub_target=gcp.cloudscheduler.JobPubsubTargetArgs(
                topic_name=pubsub.id,
                data=b64encode_str('Run the functions'),
            ),
            # 3rd day of the month
            schedule='0 0 3 * *',
            project=self.config.billing.gcp.project_id,
            region=self.config.gcp.region,
            time_zone='Australia/Sydney',
            opts=pulumi.ResourceOptions(depends_on=[self.scheduler_service]),
        )

        _ = self.create_cloud_function(
            resource_name='billing-monthly-aggregator-function',
            name='monthly-aggregator',
            service_account=self.config.billing.coordinator_machine_account,
            pubsub_topic=pubsub,
            source_archive_object=source_archive_object,
            notification_channel=self.slack_channel,
            project=self.config.billing.gcp.project_id,
            env={
                # 'SETUP_GCP_LOGGING': 'true',
                'OUTPUT_BILLING_SHEET': self.config.billing.aggregator.billing_sheet_id,
                'BQ_MONTHLY_SUMMARY_TABLE': self.config.billing.aggregator.monthly_summary_table,
            },
        )

    def setup_aggregator_functions(self):
        """Setup hourly aggregator functions"""
        assert self.config.billing
        if not 0 < self.config.billing.aggregator.interval_hours <= 24:  # noqa: PLR2004
            raise ValueError(
                f'Invalid aggregator interval, {self.config.billing.aggregator.interval_hours} '
                f'hours (0, 24]',
            )

        if 24 % self.config.billing.aggregator.interval_hours != 0:
            print(
                f'The aggregator interval ({self.config.billing.aggregator.interval_hours}hrs) '
                f'does not cleanly fit into 24 hours, this means there might be '
                f'two runs within the interval period',
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
            'billing-aggregator-topic',
            project=self.config.billing.gcp.project_id,
            opts=pulumi.ResourceOptions(depends_on=[self.pubsub_service]),
        )

        # Create a cron job to run the aggregator function on some interval
        _ = gcp.cloudscheduler.Job(
            'billing-aggregator-scheduler-job',
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
            # Balance CPU by this table:
            # https://cloud.google.com/functions/docs/configuring/memory
            memory = '1024M'
            cpu = 1
            if function == 'hail':
                memory = '2048M'
            if function == 'seqr':
                # 2GB is not enough for seqr
                memory = '2560M'
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
                cpu=cpu,
                project=self.config.billing.gcp.project_id,
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
        project: str | None = None,
        memory: str = '512M',
        cpu: int | None = None,
    ):
        """
        Create a single Cloud Function. Include the pubsub trigger and event alerts
        """

        assert self.config.billing

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
                # this one is set on an output, so specifying it keeps the function
                # from being updated, or appearing to update
                docker_repository=f'projects/{project}/locations/australia-southeast1/repositories/gcf-artifacts',
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
                available_cpu=cpu,
                timeout_seconds=540,
                environment_variables=env,
                ingress_settings='ALLOW_INTERNAL_ONLY',
                all_traffic_on_latest_revision=True,
                service_account_email=service_account,
            ),
            project=self.config.billing.gcp.project_id,
            location=self.config.gcp.region,
            opts=pulumi.ResourceOptions(
                depends_on=[self.functions_service, self.build_service],
            ),
        )

        # Slack notifications
        filter_string = fxn.name.apply(
            lambda fxn_name: f"""
                ((
                    resource.type="cloud_function"
                    AND resource.labels.function_name="{fxn_name}"
                ) OR (
                    resource.type="cloud_run_revision"
                    AND resource.labels.service_name="{fxn_name}"
                ))
                AND severity>=WARNING
            """,
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
                        period='300s',
                    )
                ),
            ),
            opts=pulumi.ResourceOptions(depends_on=[fxn]),
        )

        return fxn, trigger, alert_policy

    def setup_update_budget(self):
        assert self.config.billing
        assert self.config.gcp

        # The Cloud Function source code itself needs to be zipped up into an
        # archive, which we create using the pulumi.AssetArchive primitive.
        archive = archive_folder(PATH_TO_UPDATE_BUDGET_SOURCE_CODE)
        # Create the single Cloud Storage object, which contains the source code
        source_archive_object = gcp.storage.BucketObject(
            'billing-update-budget-source-code',
            # updating the source archive object does not trigger the cloud function
            # to actually updating the source because it's based on the name,
            # allow Pulumi to create a new name each time it gets updated
            bucket=self.source_bucket.name,
            source=archive,
            opts=pulumi.ResourceOptions(replace_on_changes=['*']),
        )

        pubsub = gcp.pubsub.Topic(
            'billing-update-budget-topic',
            project=self.config.billing.gcp.project_id,
            opts=pulumi.ResourceOptions(depends_on=[self.pubsub_service]),
        )

        # Create a cron job to run the budget update function on some interval
        _ = gcp.cloudscheduler.Job(
            'billing-update-budget-scheduler-job',
            pubsub_target=gcp.cloudscheduler.JobPubsubTargetArgs(
                topic_name=pubsub.id,
                data=b64encode_str('Run the functions'),
            ),
            # Run daily at 3am
            schedule='0 3 * * *',
            project=self.config.billing.gcp.project_id,
            region=self.config.gcp.region,
            time_zone='Australia/Sydney',
            opts=pulumi.ResourceOptions(depends_on=[self.scheduler_service]),
        )

        _ = self.create_cloud_function(
            resource_name='billing-update-budget-function',
            name='update-budget',
            service_account=self.config.billing.coordinator_machine_account,
            pubsub_topic=pubsub,
            source_archive_object=source_archive_object,
            notification_channel=self.slack_channel,
            project=self.config.billing.gcp.project_id,
            env={
                'BILLING_ACCOUNT_ID': self.config.billing.gcp.account_id,
                # TODO create new config property for this
                'BQ_BILLING_MONTHLY_BUDGET_TABLE': (
                    f'{self.config.billing.gcp.project_id}.billing.budget_by_project_monthly'
                ),
            },
        )

    def extract_dataset_table(self):
        expected_table_name_parts = 3
        table_full_name = self.config.billing.aggregator.destination_bq_table.split('.')
        if len(table_full_name) != expected_table_name_parts:
            raise ValueError(
                'Invalid destination_bq_table, should be in the format: '
                'project_id.dataset_id.table_id',
            )

        if table_full_name[0] != self.config.billing.gcp.project_id:
            raise ValueError(
                'Invalid destination_bq_table, project_id does not match the '
                'billing project_id',
            )

        # projectid, dataset_id, table_id
        return table_full_name[0], table_full_name[1], table_full_name[2]

    @cached_property
    def aggregate_table(self):
        """
        This function creates BQ aggregate table

        self.config.billing.aggregator.destination_bq_table
        has format:
        project_id.dataset_id.table_id
        """
        (project_id, dataset_id, table_id) = self.extract_dataset_table()

        # Load schema from a JSON file
        schema = get_file_content(
            f'{PATH_TO_AGGREGATE_SOURCE_CODE}/aggregate_schema.json',
        )

        # Create a BigQuery Table with clustering, time-based partitioning
        return gcp.bigquery.Table(
            f'billing-{table_id}-table',
            dataset_id=dataset_id,
            table_id=table_id,
            schema=schema,
            project=project_id,
            clusterings=['topic'],
            time_partitioning={'type': 'DAY', 'field': 'usage_end_time'},
            # This table is significantly large and recreating it takes a long time
            # so we enable deletion protection in case of accidental deletion
            # if you want to delete it, you need to disable deletion protection first
            deletion_protection=False,
        )

    def setup_materialized_views(self):
        """
        Create materlized views for the aggregate table
        """
        (project_id, dataset_id, _table_id) = self.extract_dataset_table()

        materialized_views = ['aggregate_daily', 'aggregate_daily_extended']
        for view_name in materialized_views:
            materialized_view_query = get_file_content(
                f'{PATH_TO_AGGREGATE_SOURCE_CODE}/{view_name}_view.txt',
            ).replace(
                '%AGGREGATE_TABLE%',
                self.config.billing.aggregator.destination_bq_table,
            )

            cluster_by = ['topic', 'gcp_project']
            if view_name == 'aggregate_daily_extended':
                cluster_by = ['ar_guid', 'batch_id']

            _ = gcp.bigquery.Table(
                f'{view_name}_view',
                dataset_id=dataset_id,
                table_id=view_name,
                project=project_id,
                materialized_view=gcp.bigquery.TableMaterializedViewArgs(
                    query=materialized_view_query,
                    enable_refresh=True,
                    refresh_interval_ms=1800000,
                ),
                # Define time-based partitioning on 'purchaseDate' field
                clusterings=cluster_by,
                time_partitioning={'type': 'DAY', 'field': 'day'},
                # depends_on
                opts=pulumi.ResourceOptions(depends_on=[self.aggregate_table]),
            )


def b64encode_str(s: str) -> str:
    return b64encode(s.encode('utf-8')).decode('utf-8')
