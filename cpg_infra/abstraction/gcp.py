# pylint: disable=missing-class-docstring,missing-function-docstring,too-many-public-methods
"""
GCP implementation for abstract infrastructure
"""
import base64
from datetime import date
from functools import cached_property
from typing import Any

import pulumi
import pulumi_gcp as gcp

from cpg_infra.abstraction.base import (
    CloudInfraBase,
    UNDELETE_PERIOD_IN_DAYS,
    ARCHIVE_PERIOD_IN_DAYS,
    TMP_BUCKET_PERIOD_IN_DAYS,
    BucketMembership,
    SecretMembership,
    ContainerRegistryMembership,
    BUCKET_DELETE_INCOMPLETE_UPLOAD_PERIOD_IN_DAYS,
    MachineAccountRole,
)
from cpg_infra.abstraction.group_settings import GroupSettings
from cpg_infra.config import CPGDatasetConfig, CPGInfrastructureConfig


class GcpInfrastructure(CloudInfraBase):
    @staticmethod
    def name():
        return 'gcp'

    def __init__(
        self, config: CPGInfrastructureConfig, dataset_config: CPGDatasetConfig
    ):
        super().__init__(config=config, dataset_config=dataset_config)
        self.region = config.gcp.region
        if dataset_config and dataset_config.gcp.region:
            self.region = dataset_config.gcp.region

    @cached_property
    def organization(self):
        return gcp.organizations.get_organization(domain=self.config.domain)

    @cached_property
    def project_id(self):
        return self.dataset_config.gcp.project

    @cached_property
    def project(self):
        return gcp.organizations.get_project(self.project_id)

    def get_dataset_project_id(self):
        return self.project_id

    def finalise(self):
        # Make sure this API is initialised somewhere
        _ = self._svc_serviceusage

    @staticmethod
    def member_id(member):
        if isinstance(member, gcp.serviceaccount.Account):
            return member.email

        if isinstance(member, gcp.cloudidentity.Group):
            return member.group_key.id

        if isinstance(member, str):
            return member

        if isinstance(member, pulumi.Output):
            return member

        raise NotImplementedError(f'Invalid member type {type(member)}')

    # region SERVICES
    @cached_property
    def _svc_cloudresourcemanager(self):
        return gcp.projects.Service(
            self.get_pulumi_name(f'cloudresourcemanager-service'),
            service='cloudresourcemanager.googleapis.com',
            disable_on_destroy=False,
            project=self.project_id,
        )

    @cached_property
    def _svc_cloudidentity(self):
        return gcp.projects.Service(
            self.get_pulumi_name('cloudidentity-service'),
            service='cloudidentity.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
            project=self.project_id,
        )

    @cached_property
    def _svc_serviceusage(self):
        return gcp.projects.Service(
            self.get_pulumi_name('serviceusage-service'),
            service='serviceusage.googleapis.com',
            disable_on_destroy=False,
            project=self.project_id,
        )

    @cached_property
    def _svc_secretmanager(self):
        return gcp.projects.Service(
            self.get_pulumi_name('secretmanager-service'),
            service='secretmanager.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
            project=self.project_id,
        )

    @cached_property
    def _svc_dataproc(self):
        return gcp.projects.Service(
            self.get_pulumi_name('dataproc-service'),
            service='dataproc.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
            project=self.project_id,
        )

    @cached_property
    def _svc_lifescienceapi(self):
        return gcp.projects.Service(
            self.get_pulumi_name('lifesciences-service'),
            service='lifesciences.googleapis.com',
            disable_on_destroy=False,
            project=self.project_id,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_serviceusage]),
        )

    @cached_property
    def _svc_cloudbilling(self):
        return gcp.projects.Service(
            self.get_pulumi_name('cloudbilling-service'),
            service='cloudbilling.googleapis.com',
            disable_on_destroy=False,
            project=self.project_id,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
        )

    @cached_property
    def _svc_cloudbillingbudgets(self):
        return gcp.projects.Service(
            self.get_pulumi_name('cloudbillingbudgets-service'),
            service='billingbudgets.googleapis.com',
            disable_on_destroy=False,
            project=self.project_id,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudbilling]),
        )

    @cached_property
    def _svc_iam(self):
        return gcp.projects.Service(
            self.get_pulumi_name('iam-service'),
            service='iam.googleapis.com',
            disable_on_destroy=False,
            project=self.project_id,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
        )

    # endregion SERVICES

    def create_project(self, name):
        return gcp.organizations.Project(
            # manually construct this one, because it might not be the current dataset
            f'{self.name()}-{name}-project',
            org_id=self.organization.org_id,
            project_id=name,
            name=name,
            billing_account=self.config.billing.gcp.account_id,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudbilling], protect=True
            ),
        )

    def create_budget(self, resource_key: str, *, project, budget: int, budget_filter):
        kwargs = {}
        if self.config.gcp.budget_notification_pubsub:
            kwargs['threshold_rules'] = [
                gcp.billing.BudgetThresholdRuleArgs(threshold_percent=threshold)
                for threshold in self.config.budget_notification_thresholds
            ]
            kwargs['all_updates_rule'] = gcp.billing.BudgetAllUpdatesRuleArgs(
                pubsub_topic=self.config.gcp.budget_notification_pubsub,
                schema_version='1.0',
            )

        gcp.billing.Budget(
            self.get_pulumi_name(resource_key),
            amount=gcp.billing.BudgetAmountArgs(
                specified_amount=gcp.billing.BudgetAmountSpecifiedAmountArgs(
                    units=str(budget),
                    currency_code=self.config.budget_currency,
                    nanos=0,
                )
            ),
            billing_account=self.config.billing.gcp.account_id,
            display_name=(project or self.project).name,
            budget_filter=budget_filter,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudbillingbudgets]
            ),
            **kwargs,
        )

    def create_fixed_budget(
        self,
        resource_key: str,
        *,
        project,
        budget: int,
        start_date: date = date(2022, 1, 1),
    ):
        project = project or self.project
        return self.create_budget(
            resource_key=self.get_pulumi_name(resource_key),
            budget=budget,
            budget_filter=gcp.billing.BudgetBudgetFilterArgs(
                projects=[pulumi.Output.concat('projects/', project.number)],
                # this budget applies for all time and doesn't reset
                custom_period=gcp.billing.BudgetBudgetFilterCustomPeriodArgs(
                    # arbitrary start date before all shared projects
                    start_date=gcp.billing.BudgetBudgetFilterCustomPeriodStartDateArgs(
                        year=start_date.year, month=start_date.month, day=start_date.day
                    )
                ),
            ),
            project=project,
        )

    def create_monthly_budget(self, resource_key: str, *, budget, project=None):
        project = project or self.project
        return self.create_budget(
            resource_key=self.get_pulumi_name(resource_key),
            budget=budget,
            budget_filter=gcp.billing.BudgetBudgetFilterArgs(
                projects=[pulumi.Output.concat('projects/', project.number)],
                calendar_period='MONTH',
                credit_types_treatment='INCLUDE_ALL_CREDITS',
            ),
            project=project,
        )

    def bucket_rule_undelete(self, days=UNDELETE_PERIOD_IN_DAYS) -> Any:
        return gcp.storage.BucketLifecycleRuleArgs(
            action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(
                days_since_noncurrent_time=days, with_state='ARCHIVED'
            ),
        )

    def bucket_rule_archive(self, days=ARCHIVE_PERIOD_IN_DAYS) -> Any:
        return gcp.storage.BucketLifecycleRuleArgs(
            action=gcp.storage.BucketLifecycleRuleActionArgs(
                type='SetStorageClass', storage_class='ARCHIVE'
            ),
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(age=days),
        )

    def bucket_rule_temporary(self, days=TMP_BUCKET_PERIOD_IN_DAYS) -> Any:
        return gcp.storage.BucketLifecycleRuleArgs(
            action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(age=days),
        )

    def bucket_rule_abort_incomplete_multipart_upload(
        self, days=BUCKET_DELETE_INCOMPLETE_UPLOAD_PERIOD_IN_DAYS
    ):
        """
        Lifecycle rule that deletes incomplete multipart uploads after n days
        """
        return gcp.storage.BucketLifecycleRuleArgs(
            action=gcp.storage.BucketLifecycleRuleActionArgs(
                type='AbortIncompleteMultipartUpload'
            ),
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(age=days),
        )

    @classmethod
    def storage_url_regex(cls):
        return r'^gs:\/\/'

    def bucket_output_path(self, bucket: gcp.storage.Bucket):
        return pulumi.Output.concat('gs://', bucket.name)

    def create_bucket(
        self,
        name: str,
        lifecycle_rules: list,
        unique=False,
        requester_pays=False,
        versioning: bool = True,
        autoclass: bool = False,
        project: str = None,
    ) -> Any:
        unique_bucket_name = name
        if not unique:
            unique_bucket_name = (
                f'{self.config.gcp.dataset_storage_prefix}{self.dataset}-{name}'
            )

        def autoclass_args():
            # Only set the parameter if required, to avoid superflous changes to existing buckets.
            return gcp.storage.BucketAutoclassArgs(enabled=True) if autoclass else None

        return gcp.storage.Bucket(
            self.get_pulumi_name(name + '-bucket'),
            name=unique_bucket_name,
            location=self.region,
            uniform_bucket_level_access=True,
            versioning=gcp.storage.BucketVersioningArgs(enabled=versioning),
            autoclass=autoclass_args(),
            labels={'bucket': unique_bucket_name},
            # duplicate the array to avoid adding the lifecycle rule to an existing list
            lifecycle_rules=[
                *lifecycle_rules,
                self.bucket_rule_abort_incomplete_multipart_upload(),
            ],
            requester_pays=requester_pays,
            project=project or self.project.project_id,
        )

    def get_member_key(self, member):  # pylint: disable=too-many-return-statements
        # it's a 'cpg_infra.driver.CPGInfrastructure.GroupProvider.Group'
        if hasattr(member, 'is_group') and hasattr(member, 'group'):
            # cheeky catch for internal group
            return self.get_member_key(member.group)

        if isinstance(member, gcp.serviceaccount.Account):
            return pulumi.Output.concat('serviceAccount:', member.email)

        if isinstance(member, gcp.cloudidentity.Group):
            return pulumi.Output.concat('group:', member.group_key.id)

        if isinstance(member, gcp.storage.Bucket):
            return member.name

        if isinstance(member, str):
            if member.endswith('.iam.gserviceaccount.com') and not member.startswith(
                'serviceAccount:'
            ):
                return f'serviceAccount:{member}'

            return member

        if isinstance(member, pulumi.Output):
            return member

        raise NotImplementedError(f'Invalid member type {type(member)}')

    def get_preferred_group_membership_key(self, member):
        if hasattr(member, 'is_group') and hasattr(member, 'group'):
            # cheeky catch for internal group
            return self.get_preferred_group_membership_key(member.group)
        if isinstance(member, gcp.cloudidentity.Group):
            return member.group_key.id
        if isinstance(member, gcp.serviceaccount.Account):
            return member.email
        if isinstance(member, str):
            return member

        raise NotImplementedError(
            f'Invalid preferred GroupMembership type {type(member)}'
        )

    def get_group_key(self, group):
        if isinstance(group, (gcp.serviceaccount.Account, gcp.storage.Bucket)):
            raise ValueError(f'Incorrect type for group key: {type(group)}')

        if isinstance(group, gcp.cloudidentity.Group):
            return group.id
            # return pulumi.Output.concat('group:', group.group_key.id)

        if isinstance(group, str):
            if group.endswith(
                '@' + self.config.gcp.groups_domain
            ) and not group.startswith('group:'):
                return f'group:{group}'

            return group

        if isinstance(group, pulumi.Output):
            return group

        raise NotImplementedError(f'Not valid for type {type(group)}')

    def get_secret_key(self, secret):
        if isinstance(secret, gcp.secretmanager.Secret):
            return secret.id

        if isinstance(secret, str):
            return secret

        raise NotImplementedError(f'Not valid for type {type(secret)}')

    def bucket_membership_to_role(self, membership: BucketMembership):
        if membership == BucketMembership.MUTATE:
            return 'roles/storage.admin'
        if membership == BucketMembership.APPEND:
            return f'{self.organization.id}/roles/StorageViewerAndCreator'
        if membership == BucketMembership.READ:
            return f'{self.organization.id}/roles/StorageObjectAndBucketViewer'
        if membership == BucketMembership.LIST:
            return f'{self.organization.id}/roles/StorageLister'

        raise ValueError(f'Unrecognised bucket membership type {membership}')

    def add_member_to_bucket(
        self, resource_key: str, bucket, member, membership: BucketMembership
    ) -> Any:
        gcp.storage.BucketIAMMember(
            self.get_pulumi_name(resource_key),
            bucket=self.get_member_key(bucket),
            member=self.get_member_key(member),
            role=self.bucket_membership_to_role(membership),
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def give_member_ability_to_list_buckets(
        self, resource_key: str, member, project: str = None
    ):
        gcp.projects.IAMMember(
            self.get_pulumi_name(resource_key),
            role=self.bucket_membership_to_role(BucketMembership.LIST),
            member=self.get_member_key(member),
            project=project or self.project_id,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def create_machine_account(
        self, name: str, project: str = None, *, resource_key: str = None
    ) -> Any:
        if project and isinstance(project, gcp.organizations.Project):
            project = project.project_id

        return gcp.serviceaccount.Account(
            self.get_pulumi_name(resource_key or f'service-account-{name}'),
            account_id=name,
            # display_name=name,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_iam]),
            project=project or self.project.project_id,
        )

    # pylint: disable=unused-argument
    def add_member_to_machine_account_role(
        self,
        resource_key: str,
        machine_account,
        member,
        role: MachineAccountRole,
        project=None,
    ) -> Any:
        # no actioning project, as you're adding to a specific resource

        if role == MachineAccountRole.ACCESS:
            _role = 'roles/iam.serviceAccountUser'
        elif role == MachineAccountRole.ADMIN:
            _role = 'roles/iam.serviceAccountAdmin'
        elif role == MachineAccountRole.CREDENTIALS_ADMIN:
            _role = 'roles/iam.serviceAccountKeyAdmin'
        else:
            raise ValueError(f'Unsupported member type: {role}')

        gcp.serviceaccount.IAMMember(
            self.get_pulumi_name(resource_key),
            service_account_id=machine_account.name,
            role=_role,
            member=self.get_member_key(member),
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudidentity, self._svc_iam]
            ),
        )

    def get_credentials_for_machine_account(self, resource_key, account):
        return gcp.serviceaccount.Key(
            self.get_pulumi_name(resource_key),
            service_account_id=account.email,
        ).private_key.apply(lambda s: base64.b64decode(s).decode('utf-8'))

    def create_group(self, name: str) -> Any:
        mail = f'{name}@{self.config.gcp.groups_domain}'
        group = gcp.cloudidentity.Group(
            self.get_pulumi_name(name + '-group'),
            display_name=name,
            group_key=gcp.cloudidentity.GroupGroupKeyArgs(id=mail),
            labels={'cloudidentity.googleapis.com/groups.discussion_forum': ''},
            parent=f'customers/{self.config.gcp.customer_id}',
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )
        # Allow domain-external members in the group.
        GroupSettings(
            f'{name}-group-settings',
            group_email=mail,
            settings={'allowExternalMembers': 'true'},
            opts=pulumi.resource.ResourceOptions(depends_on=[group]),
        )
        return group

    def add_group_member(
        self, resource_key: str, group, member, unique_resource_key: bool = False
    ) -> Any:
        if self.config.disable_group_memberships:
            return

        if not unique_resource_key:
            resource_key = self.get_pulumi_name(resource_key)

        gcp.cloudidentity.GroupMembership(
            resource_key,
            group=self.get_group_key(group),
            preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
                id=self.get_preferred_group_membership_key(member)
            ),
            roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def create_secret(self, name: str, project: str = None) -> Any:
        return gcp.secretmanager.Secret(
            self.get_pulumi_name(name),
            secret_id=name,
            replication=gcp.secretmanager.SecretReplicationArgs(
                user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
                    replicas=[
                        gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                            location=self.region,
                        ),
                    ],
                ),
            ),
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_secretmanager]),
            project=project or self.project_id,
        )

    def add_secret_member(
        self,
        resource_key: str,
        secret,
        member,
        membership: SecretMembership,
        project: str = None,
    ) -> Any:
        if membership == SecretMembership.ADMIN:
            role = 'roles/secretmanager.secretVersionManager'
        elif membership == SecretMembership.ACCESSOR:
            role = 'roles/secretmanager.secretAccessor'
        else:
            raise ValueError(f'Unrecognised secret membership type: {membership}')

        if isinstance(secret, gcp.secretmanager.Secret):
            secret_id = secret.id
        elif isinstance(secret, str):
            secret_id = secret
            if not secret.count('/') == 3:
                if isinstance(project, gcp.organizations.Project):
                    project = project.id
                secret_id = f'projects/{project or self.project_id}/secrets/{secret}'
        else:
            raise ValueError(f'Unexpected secret type: {secret} ({type(secret)})')

        gcp.secretmanager.SecretIamMember(
            self.get_pulumi_name(resource_key),
            project=project or self.project_id,
            secret_id=secret_id,
            role=role,
            member=self.get_member_key(member),
        )

    def add_secret_version(
        self,
        resource_key: str,
        secret: Any,
        contents: Any,
    ):
        return gcp.secretmanager.SecretVersion(
            self.get_pulumi_name(resource_key), secret=secret.id, secret_data=contents
        )

    # region CONTAINER REGISTRY

    def create_container_registry(self, name: str):
        return gcp.artifactregistry.Repository(
            self.get_pulumi_name('artifact-registry-' + name),
            repository_id=name,
            project=self.project_id,
            format='DOCKER',
            location=self.region,
        )

    def add_member_to_container_registry(
        self, resource_key: str, registry, member, membership, project=None
    ) -> Any:
        if membership == ContainerRegistryMembership.READER:
            role = 'roles/artifactregistry.reader'
        elif membership == ContainerRegistryMembership.WRITER:
            role = 'roles/artifactregistry.writer'
        else:
            raise ValueError(f'Unrecognised group membership type: {membership}')

        gcp.artifactregistry.RepositoryIamMember(
            self.get_pulumi_name(resource_key),
            project=project or self.project_id,
            location=self.region,
            repository=registry,
            role=role,
            member=self.get_member_key(member),
        )

    # endregion CONTAINER REGISTRY

    # region GCP SPECIFIC

    def add_member_to_lifescience_api(self, resource_key: str, account):
        gcp.projects.IAMMember(
            self.get_pulumi_name(resource_key),
            role='roles/lifesciences.workflowsRunner',
            member=self.get_member_key(account),
            project=self.project_id,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_lifescienceapi]),
        )

    #
    def add_member_to_dataproc_api(self, resource_key: str, account, role: str):
        if role in ('worker', 'admin'):
            role = f'roles/dataproc.{role}'

        gcp.projects.IAMMember(
            self.get_pulumi_name(resource_key),
            role=role,
            member=self.get_member_key(account),
            project=self.project_id,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_dataproc]),
        )

    def add_cloudrun_invoker(
        self, resource_key: str, *, service: str, project: str, member
    ):
        gcp.cloudrun.IamMember(
            self.get_pulumi_name(resource_key),
            location=self.region,
            project=project or self.project_id,
            service=service,
            role='roles/run.invoker',
            member=self.get_member_key(member),
        )

    def add_project_role(
        self, resource_key: str, *, member: Any, role: str, project: str = None
    ):
        gcp.projects.IAMMember(
            self.get_pulumi_name(resource_key),
            project=project or self.project_id,
            role=role,
            member=self.get_member_key(member),
        )

    def add_blob_to_bucket(self, resource_name, bucket, output_name, contents):
        return gcp.storage.BucketObject(
            # Don't uniquify resource_name here, because often
            # it's called outside the scope of a specific infra
            resource_name,
            bucket=bucket,
            name=output_name,
            content=contents,
        )

    # endregion GCP SPECIFIC
