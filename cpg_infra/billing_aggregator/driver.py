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
    - Enable Cloud Function / Cloud Run Services
    - Build the aggregator image with Cloud Build (needs gcloud where pulumi runs)
    - Create a Cloud Run job and cloud scheduler for each aggregator
    - Create a bucket for the remaining cloud functions' source code
    - Create a slack notification channel for all functions

TODO:

    - action monthly billing function
"""

import hashlib
import os
from base64 import b64encode
from functools import cached_property

import pulumi
import pulumi_command as command
import pulumi_gcp as gcp

from cpg_infra.plugin import CpgInfrastructurePlugin
from cpg_infra.utils import DEFAULT_ALLOWED_EXTENSIONS, archive_folder
from cpg_utils.cloud import read_secret

PATH_TO_AGGREGATE_SOURCE_CODE = os.path.join(os.path.dirname(__file__), 'aggregate')


def hash_folder(path: str) -> str:
    """Short hash of the files the aggregator Dockerfile copies into the image"""
    digest = hashlib.sha256()
    for filename in sorted(os.listdir(path)):
        if filename == 'Dockerfile' or filename.endswith(
            tuple(DEFAULT_ALLOWED_EXTENSIONS),
        ):
            digest.update(filename.encode())
            with open(os.path.join(path, filename), 'rb') as file:
                digest.update(file.read())
    return digest.hexdigest()[:12]


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
        # setup BQ objects
        _ = self.aggregate_table
        self.setup_materialized_views()

        self.setup_gcp_cost_control()
        self.setup_gcp_cost_reporting()

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

    def create_source_archive(
        self,
        resource_name: str,
        bucket_name: str,
        path_to_folder: str,
    ):
        # The Cloud Function source code itself needs to be zipped up into an
        # archive, which we create using the pulumi.AssetArchive primitive.
        archive = archive_folder(path_to_folder)

        # Create the single Cloud Storage object, which contains the source code
        return gcp.storage.BucketObject(
            resource_name,
            bucket=bucket_name,
            source=archive,
            opts=pulumi.ResourceOptions(replace_on_changes=['*']),
        )

    def setup_gcp_cost_control(self):
        """
        Create the gcp cost control cloud function to cut off billing when
        it exceeds the budget
        """
        assert self.config.gcp
        assert self.config.billing
        assert self.config.billing.gcp_cost_controls

        location = self.config.gcp.region

        service_account = self.config.billing.gcp_cost_controls.machine_account
        slack_channel = self.config.billing.gcp_cost_controls.slack_channel
        pubsub_topic_name = self.config.gcp.budget_notification_pubsub

        # Create source archive
        source_archive = self.create_source_archive(
            'billing-gcp-cost-control-source-code',
            self.source_bucket.name,
            os.path.join(os.path.dirname(__file__), 'gcp_cost_control'),
        )

        # Deploy Cloud Function
        env = {
            'SLACK_CHANNEL': slack_channel,
            'GCP_PROJECT': self.config.billing.gcp.project_id,
        }
        memory = '256M'
        cpu = 1

        build_config = gcp.cloudfunctionsv2.FunctionBuildConfigArgs(
            entry_point='gcp_cost_control',
            runtime='python311',
            environment_variables={
                'GOOGLE_FUNCTION_SOURCE': 'main.py',
            },
            source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceArgs(
                storage_source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceStorageSourceArgs(
                    bucket=self.source_bucket.name,
                    object=source_archive.name,
                ),
            ),
        )

        service_config = gcp.cloudfunctionsv2.FunctionServiceConfigArgs(
            max_instance_count=1,
            min_instance_count=0,
            available_memory=memory,
            available_cpu=cpu,
            timeout_seconds=540,
            environment_variables=env,
            ingress_settings='ALLOW_INTERNAL_ONLY',
            all_traffic_on_latest_revision=True,
            service_account_email=service_account,
        )

        _ = gcp.cloudfunctionsv2.Function(
            'gcp-cost-control',
            location=location,
            service_config=service_config,
            build_config=build_config,
            event_trigger=gcp.cloudfunctionsv2.FunctionEventTriggerArgs(
                event_type='google.cloud.pubsub.topic.v1.messagePublished',
                pubsub_topic=pubsub_topic_name,
                retry_policy='RETRY_POLICY_DO_NOT_RETRY',
                service_account_email=service_account,
            ),
        )

    def setup_gcp_cost_reporting(self):
        """
        Create the slack bot gcp-cost-control in the CPG slack
        to notify us of projects that are approaching their monthly budget
        as well as monitor the hail billing account
        """

        assert self.config.billing
        assert self.config.billing.gcp_cost_controls
        assert self.config.gcp

        project_id = self.config.billing.gcp.project_id
        bigquery_billing_table = self.config.billing.gcp.source_bq_table
        billing_account_id = self.config.billing.gcp.account_id
        region = self.config.gcp.region

        time_zone = self.config.billing.gcp_cost_controls.timezone
        service_account = self.config.billing.gcp_cost_controls.machine_account
        slack_channel = self.config.billing.gcp_cost_controls.slack_channel

        # Create source archive
        source_archive = self.create_source_archive(
            'billing-gcp-cost-report-source-code',
            self.source_bucket.name,
            os.path.join(os.path.dirname(__file__), 'gcp_cost_report_slack_bot'),
        )

        # Deploy Cloud Function
        function = self.create_cloud_function(
            name='GCP Cost Reporting',
            resource_name='gcp-cost-reporting-cloud-function',
            project=self.config.billing.gcp.project_id,
            service_account=service_account,
            notification_channel=self.slack_channel,
            source_bucket=self.source_bucket.name,
            source_archive_object=source_archive,
            source_file='main.py',
            entry_point='slack_bot_cost_report',
            env={
                'QUERY_TIME_ZONE': time_zone,
                'SLACK_CHANNEL': slack_channel,
                'BIGQUERY_BILLING_TABLE': bigquery_billing_table,
                'BILLING_ACCOUNT_ID': billing_account_id,
            },
            memory='256M',
        )

        # Create Cloud Scheduler job
        _ = gcp.cloudscheduler.Job(
            'gcp-cost-reporting-job',
            project=project_id,
            region=region,
            time_zone=time_zone,
            description='Triggers a daily cost report Cloud Function',
            schedule='0 9 * * *',
            http_target=gcp.cloudscheduler.JobHttpTargetArgs(
                uri=function.url,
                http_method='POST',
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                oidc_token=gcp.cloudscheduler.JobHttpTargetOidcTokenArgs(
                    audience=function.url,
                    service_account_email=service_account,
                ),
            ),
            opts=pulumi.ResourceOptions(depends_on=[self.scheduler_service]),
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

        project = self.config.billing.gcp.project_id
        region = self.config.gcp.region

        for function in self.config.billing.aggregator.functions:
            # Balance CPU by this table:
            # https://cloud.google.com/functions/docs/configuring/memory
            memory = '1024M'
            cpu = 1
            timeout = 540
            default_interval_hours = self.config.billing.aggregator.interval_hours

            # TODO: We should consider moving function specific cpu/memory/timeout values to config
            # Scheduler start minute, staggered to avoid all functions running at the same time
            start_minute = 0

            if function in ['gcp']:
                # GCP function can handle more than an hour of data aggregation and
                # there seems to be inconsistency how often Google insert new billing records
                # From Google docs:
                # "In summary, while the system works to provide relatively fresh data,
                # it is not a real-time (within minutes) stream, and you should design your analysis
                # around the understanding that data might be delayed by a few hours."
                #
                # atm default_interval_hours is usually set to 1H,
                # 8H was a good start as interval for GCP, however
                # based on billing records investigation 8H only covers around 80%,
                # 24H should give us close to 100% of all the billing records.
                # Again, we might need to extend if we find that is not sufficient
                default_interval_hours = 24 * default_interval_hours
                start_minute = 1

            if function in ['hail', 'seqr', 'seqr24', 'gcp']:
                # finish 5 minutes before the next scheduled run so runs can't overlap
                timeout = self.config.billing.aggregator.interval_hours * 3600 - 300

            if function == 'hail':
                # hail specific aggreg function needs over 4GB of memory
                # 4GB per 1x cpu
                cpu = 2
                memory = '8Gi'
                start_minute = 2

            if function in ['seqr', 'seqr24']:
                # seqr specific aggreg function needs over 8GB of memory
                # 4GB per 1x cpu
                cpu = 4
                memory = '16Gi'
                start_minute = 3 if function == 'seqr' else 4

            job = self.create_aggregator_job(
                function=function,
                memory=memory,
                cpu=cpu,
                timeout=timeout,
                env={
                    'GCP_AGGREGATE_DEST_TABLE': self.config.billing.aggregator.destination_bq_table,
                    'GCP_BILLING_SOURCE_TABLE': self.config.billing.gcp.source_bq_table,
                    'DEFAULT_INTERVAL_HOURS': str(default_interval_hours),
                    'BILLING_PROJECT_ID': self.config.billing.gcp.project_id,
                    'ICA_RAW_TABLE': self.config.billing.aggregator.ica_raw_table,
                    'ICA_API_SECRET_NAME': self.config.billing.aggregator.ica_api_secret_name,
                },
            )
            # :run only starts the execution and returns straight away. No retries:
            # if the response were lost, a retry would start a second execution
            # alongside the first, and concurrent runs can insert duplicates
            _ = gcp.cloudscheduler.Job(
                f'billing-aggregator-scheduler-job-{function}',
                http_target=gcp.cloudscheduler.JobHttpTargetArgs(
                    uri=job.name.apply(
                        lambda job_name: (
                            f'https://run.googleapis.com/v2/projects/{project}'
                            f'/locations/{region}/jobs/{job_name}:run'
                        ),
                    ),
                    http_method='POST',
                    body=b64encode(b'{}').decode(),
                    headers={'Content-Type': 'application/json'},
                    oauth_token=gcp.cloudscheduler.JobHttpTargetOauthTokenArgs(
                        service_account_email=self.config.billing.coordinator_machine_account,
                    ),
                ),
                schedule=f'{start_minute} */{self.config.billing.aggregator.interval_hours} * * *',
                retry_config=gcp.cloudscheduler.JobRetryConfigArgs(retry_count=0),
                attempt_deadline='30s',
                project=project,
                region=region,
                time_zone='Australia/Sydney',
                opts=pulumi.ResourceOptions(depends_on=[job, self.scheduler_service]),
            )

            # no successful execution for two consecutive scheduled runs
            missed_runs_hours = min(
                2 * self.config.billing.aggregator.interval_hours, 23
            )
            _ = gcp.monitoring.AlertPolicy(
                f'billing-aggregator-{function}-missed-runs-alert',
                display_name=f'Aggregator {function.capitalize()} Billing Job Missed Runs',
                combiner='OR',
                notification_channels=[self.slack_channel.id],
                conditions=[
                    gcp.monitoring.AlertPolicyConditionArgs(
                        display_name=f'No successful run in {missed_runs_hours}h',
                        condition_absent=gcp.monitoring.AlertPolicyConditionConditionAbsentArgs(
                            filter=job.name.apply(
                                lambda job_name: (
                                    'metric.type="run.googleapis.com/job/completed_execution_count" '
                                    'AND resource.type="cloud_run_job" '
                                    f'AND resource.label.job_name="{job_name}" '
                                    'AND metric.label.result="succeeded"'
                                ),
                            ),
                            duration=f'{missed_runs_hours * 3600}s',
                            aggregations=[
                                gcp.monitoring.AlertPolicyConditionConditionAbsentAggregationArgs(
                                    alignment_period='300s',
                                    per_series_aligner='ALIGN_SUM',
                                    cross_series_reducer='REDUCE_SUM',
                                    group_by_fields=['resource.label.job_name'],
                                ),
                            ],
                        ),
                    ),
                ],
                opts=pulumi.ResourceOptions(depends_on=[job]),
            )

    @cached_property
    def run_service(self):
        return gcp.projects.Service(
            'billing-aggregator-run-service',
            service='run.googleapis.com',
            project=self.config.billing.gcp.project_id,
            disable_on_destroy=False,
        )

    @cached_property
    def artifact_registry_service(self):
        return gcp.projects.Service(
            'billing-aggregator-artifactregistry-service',
            service='artifactregistry.googleapis.com',
            project=self.config.billing.gcp.project_id,
            disable_on_destroy=False,
        )

    @cached_property
    def aggregator_image(self) -> pulumi.Output[str]:
        """
        Build the aggregator image with Cloud Build, tagged with a hash of the
        source so it's only rebuilt (and the jobs updated) when the code changes
        """
        project = self.config.billing.gcp.project_id
        region = self.config.gcp.region

        repository = gcp.artifactregistry.Repository(
            'billing-aggregator-image-repository',
            repository_id='billing-aggregator',
            format='DOCKER',
            location=region,
            project=project,
            opts=pulumi.ResourceOptions(depends_on=[self.artifact_registry_service]),
        )
        tag = hash_folder(PATH_TO_AGGREGATE_SOURCE_CODE)
        image = repository.repository_id.apply(
            lambda repository_id: (
                f'{region}-docker.pkg.dev/{project}/{repository_id}/aggregator:{tag}'
            ),
        )
        # only runs on `pulumi up` (not preview), and again when the tag changes
        build = command.local.Command(
            'billing-aggregator-image-build',
            create=image.apply(
                lambda image_name: (
                    f'gcloud builds submit {PATH_TO_AGGREGATE_SOURCE_CODE} '
                    f'--tag={image_name} --project={project} --region={region}'
                ),
            ),
            opts=pulumi.ResourceOptions(depends_on=[repository, self.build_service]),
        )
        return pulumi.Output.all(image, build.id).apply(lambda args: args[0])

    def create_aggregator_job(
        self,
        function: str,
        memory: str,
        cpu: int,
        timeout: int,
        env: dict[str, str],
    ) -> gcp.cloudrunv2.Job:
        """
        Create the Cloud Run job for one aggregator, plus its error alert
        """
        project = self.config.billing.gcp.project_id
        region = self.config.gcp.region
        service_account = self.config.billing.coordinator_machine_account

        job = gcp.cloudrunv2.Job(
            f'billing-aggregator-{function}-job',
            location=region,
            project=project,
            template=gcp.cloudrunv2.JobTemplateArgs(
                template=gcp.cloudrunv2.JobTemplateTemplateArgs(
                    containers=[
                        gcp.cloudrunv2.JobTemplateTemplateContainerArgs(
                            image=self.aggregator_image,
                            args=[f'{function}.py'],
                            envs=[
                                gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                    name=name,
                                    value=value,
                                )
                                for name, value in env.items()
                            ],
                            resources=gcp.cloudrunv2.JobTemplateTemplateContainerResourcesArgs(
                                limits={'cpu': str(cpu), 'memory': memory},
                            ),
                        ),
                    ],
                    timeout=f'{timeout}s',
                    # a retried task could still be running when the next hourly
                    # execution starts, and concurrent runs can insert duplicates
                    max_retries=0,
                    service_account=service_account,
                ),
            ),
            opts=pulumi.ResourceOptions(depends_on=[self.run_service]),
        )

        _ = gcp.cloudrunv2.JobIamMember(
            f'billing-aggregator-{function}-job-invoker',
            name=job.name,
            location=region,
            project=project,
            role='roles/run.invoker',
            member=f'serviceAccount:{service_account}',
        )

        _ = gcp.monitoring.AlertPolicy(
            f'billing-aggregator-{function}-alert',
            display_name=f'Aggregator {function.capitalize()} Billing Job Error Alert',
            combiner='OR',
            notification_channels=[self.slack_channel.id],
            conditions=[
                gcp.monitoring.AlertPolicyConditionArgs(
                    display_name='Job warning/error',
                    condition_matched_log=gcp.monitoring.AlertPolicyConditionConditionMatchedLogArgs(
                        filter=job.name.apply(
                            lambda job_name: (
                                'resource.type="cloud_run_job" '
                                f'AND resource.labels.job_name="{job_name}" '
                                'AND severity>=WARNING'
                            ),
                        ),
                    ),
                ),
            ],
            alert_strategy=gcp.monitoring.AlertPolicyAlertStrategyArgs(
                notification_rate_limit=(
                    gcp.monitoring.AlertPolicyAlertStrategyNotificationRateLimitArgs(
                        period='300s',
                    )
                ),
            ),
            opts=pulumi.ResourceOptions(depends_on=[job]),
        )

        return job

    # monthly billing aggregator

    def create_cloud_function(
        self,
        resource_name: str,
        name: str,
        service_account: str,
        source_archive_object: gcp.storage.BucketObject,
        notification_channel: gcp.monitoring.NotificationChannel,
        env: dict,
        source_bucket: str | None = None,
        source_file: str | None = None,
        entry_point: str | None = 'from_request',
        project: str | None = None,
        memory: str = '512M',
        cpu: int | None = None,
        timeout: int = 540,
    ) -> gcp.cloudfunctionsv2.Function:
        """
        Create a single Cloud Function. Include the http trigger and event alerts
        """

        assert self.config.billing

        # Create the Cloud Function

        build_environment_variables = {}
        if source_file:
            build_environment_variables['GOOGLE_FUNCTION_SOURCE'] = source_file

        service_config = gcp.cloudfunctionsv2.FunctionServiceConfigArgs(
            max_instance_count=1,
            min_instance_count=0,
            available_memory=memory,
            available_cpu=str(cpu) if cpu else None,
            timeout_seconds=timeout,
            environment_variables=env,
            ingress_settings='ALLOW_INTERNAL_ONLY',
            all_traffic_on_latest_revision=True,
            service_account_email=service_account,
        )
        fxn = gcp.cloudfunctionsv2.Function(
            resource_name,
            build_config=gcp.cloudfunctionsv2.FunctionBuildConfigArgs(
                runtime='python311',
                entry_point=entry_point,
                environment_variables=build_environment_variables,
                # this one is set on an output, so specifying it keeps the function
                # from being updated, or appearing to update
                docker_repository=f'projects/{project}/locations/australia-southeast1/repositories/gcf-artifacts',
                source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceArgs(
                    storage_source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceStorageSourceArgs(
                        bucket=source_bucket,
                        object=source_archive_object.name,
                    ),
                ),
            ),
            service_config=service_config,
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
                AND NOT httpRequest.status=429
            """,
        )
        # 429s above are cold-start rejections, retried by the Cloud Scheduler job

        # Create the Cloud Function's event alert
        alert_condition = gcp.monitoring.AlertPolicyConditionArgs(
            condition_matched_log=(
                gcp.monitoring.AlertPolicyConditionConditionMatchedLogArgs(
                    filter=filter_string,
                )
            ),
            display_name='Function warning/error',
        )

        alert_policy_name = 'billing-' + name.lower().replace(' ', '-') + '-alert'
        _ = gcp.monitoring.AlertPolicy(
            alert_policy_name,
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

        return fxn

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
        Create materialized views for the aggregate table
        TODO:
        BQ does not allow views updates, we need to come with better ways of updating the views
        for time being this function will be commented out
        all views would be setup/updated manually
        """

        # (project_id, dataset_id, _table_id) = self.extract_dataset_table()

        # materialized_views = ['aggregate_daily', 'aggregate_daily_extended']
        # for view_name in materialized_views:
        #     materialized_view_query = get_file_content(
        #         f'{PATH_TO_AGGREGATE_SOURCE_CODE}/{view_name}_view.txt',
        #     ).replace(
        #         '%AGGREGATE_TABLE%',
        #         self.config.billing.aggregator.destination_bq_table,
        #     )

        #     cluster_by = ['topic', 'gcp_project']
        #     if view_name == 'aggregate_daily_extended':
        #         cluster_by = ['ar_guid', 'batch_id']

        #     _ = gcp.bigquery.Table(
        #         f'{view_name}_view',
        #         dataset_id=dataset_id,
        #         table_id=view_name,
        #         project=project_id,
        #         materialized_view=gcp.bigquery.TableMaterializedViewArgs(
        #             query=materialized_view_query,
        #             enable_refresh=True,
        #             refresh_interval_ms=1800000,
        #         ),
        #         # to be able update schema we need to disable deletion protection
        #         # views can be regenerated, there is not need to protect them
        #         deletion_protection=False,
        #         # Define time-based partitioning on 'purchaseDate' field
        #         clusterings=cluster_by,
        #         time_partitioning={'type': 'DAY', 'field': 'day'},
        #         # depends_on
        #         opts=pulumi.ResourceOptions(depends_on=[self.aggregate_table]),
        #     )

        return


def b64encode_str(s: str) -> str:
    return b64encode(s.encode('utf-8')).decode('utf-8')
