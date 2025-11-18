# flake8: noqa: ERA001,ANN001,ANN102,ANN202,ANN205,ANN206,ANN401,ARG002
"""
GCP implementation for abstract infrastructure
"""

import base64
from datetime import date
from functools import cached_property
from typing import Any, NamedTuple, Optional

import pulumi
import pulumi_gcp as gcp

from cpg_infra.abstraction.base import (
    ARCHIVE_PERIOD_IN_DAYS,
    BUCKET_DELETE_INCOMPLETE_UPLOAD_PERIOD_IN_DAYS,
    TMP_BUCKET_PERIOD_IN_DAYS,
    UNDELETE_PERIOD_IN_DAYS,
    BucketMembership,
    CloudInfraBase,
    CloudName,
    ContainerRegistryMembership,
    MachineAccountRole,
    SecretMembership,
)
from cpg_infra.abstraction.google_group_membership import (
    GoogleGroupMembership,
    GoogleGroupMembershipInputs,
)
from cpg_infra.abstraction.google_group_settings import GoogleGroupSettings
from cpg_infra.config import CPGDatasetConfig, CPGInfrastructureConfig


class BucketMembershipRole(NamedTuple):
    role: str
    resource_key: str


def get_member_key(member):  # pylint: disable=too-many-return-statements
    # it's a 'cpg_infra.driver.CPGInfrastructure.GroupProvider.Group'
    if isinstance(member, pulumi.Output):
        return pulumi.Output.apply(member, get_member_key)

    if hasattr(member, 'is_group') and hasattr(member, 'group'):
        # cheeky catch for internal group
        return get_member_key(member.group)

    if isinstance(member, gcp.serviceaccount.Account):
        return pulumi.Output.concat('serviceAccount:', member.email)

    if isinstance(member, gcp.cloudidentity.Group):
        return pulumi.Output.concat('group:', member.group_key.id)

    if isinstance(member, gcp.storage.Bucket):
        return member.name

    if isinstance(member, str):
        if member.endswith('.iam.gserviceaccount.com') and not member.startswith(
            'serviceAccount:',
        ):
            return f'serviceAccount:{member}'

        return member

    raise NotImplementedError(f'Invalid member type {type(member)}')


def get_preferred_group_membership_key(member) -> str | pulumi.Output[str]:
    if isinstance(member, pulumi.Output):
        return pulumi.Output.apply(
            member,
            get_preferred_group_membership_key,
        )

    if hasattr(member, 'is_group') and hasattr(member, 'group'):
        # cheeky catch for internal group
        return get_preferred_group_membership_key(member.group)
    if isinstance(member, gcp.cloudidentity.Group):
        return member.group_key.id
    if isinstance(member, gcp.serviceaccount.Account):
        return member.email
    if isinstance(member, str):
        return member

    raise NotImplementedError(
        f'Invalid preferred GroupMembership type {type(member)}',
    )


class GcpInfrastructure(CloudInfraBase):
    @staticmethod
    def name() -> CloudName:
        return 'gcp'

    def __init__(
        self,
        config: CPGInfrastructureConfig,
        dataset_config: CPGDatasetConfig,
    ) -> None:
        assert config.gcp
        super().__init__(config=config, dataset_config=dataset_config)
        self.region = config.gcp.region
        if dataset_config and dataset_config.gcp.region:
            self.region = dataset_config.gcp.region

    @cached_property
    def organization(self):
        return gcp.organizations.get_organization(domain=self.config.domain)

    def get_project(self):
        return self.create_project(
            resource_key='project',
            name=self.dataset_config.gcp.project or self.dataset,
        )

    def get_project_id(self):
        return self.project.project_id

    def finalise(self):
        # Make sure this API is initialised somewhere
        _ = self._svc_serviceusage

    @staticmethod
    def member_id(member) -> str | pulumi.Output[str]:
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
            self.get_pulumi_name('cloudresourcemanager-service'),
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
                depends_on=[self._svc_cloudresourcemanager],
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
                depends_on=[self._svc_cloudresourcemanager],
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
                depends_on=[self._svc_cloudresourcemanager],
            ),
            project=self.project_id,
        )

    @cached_property
    def _svc_batchapi(self):
        return gcp.projects.Service(
            self.get_pulumi_name('batch-service'),
            service='batch.googleapis.com',
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
                depends_on=[self._svc_cloudresourcemanager],
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
                depends_on=[self._svc_cloudresourcemanager],
            ),
        )

    # endregion SERVICES

    def create_project(self, resource_key, name):
        opts = None
        if 'shared' in resource_key:
            # temporary manual rename for shared projects
            opts = pulumi.ResourceOptions(
                aliases=[
                    pulumi.Alias(
                        name=f'gcp-{self.dataset_config.gcp.project}-shared-project',
                    ),
                ],
            )

        return gcp.organizations.Project(
            self.get_pulumi_name(resource_key),
            org_id=self.organization.org_id,
            project_id=name,
            name=name,
            billing_account=self.config.billing.gcp.account_id,
            opts=opts,
            # deleting projects is pretty tedious, so let's just NOT do that.
            skip_delete=True,
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
                ),
            ),
            billing_account=self.config.billing.gcp.account_id,
            display_name=(project or self.project).name,
            budget_filter=budget_filter,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudbillingbudgets],
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
                        year=start_date.year,
                        month=start_date.month,
                        day=start_date.day,
                    ),
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
                days_since_noncurrent_time=days,
                with_state='ARCHIVED',
            ),
        )

    def bucket_rule_archive(self, days=ARCHIVE_PERIOD_IN_DAYS) -> Any:
        return gcp.storage.BucketLifecycleRuleArgs(
            action=gcp.storage.BucketLifecycleRuleActionArgs(
                type='SetStorageClass',
                storage_class='ARCHIVE',
            ),
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(age=days),
        )

    def bucket_rule_temporary(self, days=TMP_BUCKET_PERIOD_IN_DAYS) -> Any:
        return gcp.storage.BucketLifecycleRuleArgs(
            action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(age=days),
        )

    def bucket_rule_abort_incomplete_multipart_upload(
        self,
        days=BUCKET_DELETE_INCOMPLETE_UPLOAD_PERIOD_IN_DAYS,
    ):
        """
        Lifecycle rule that deletes incomplete multipart uploads after n days
        """
        return gcp.storage.BucketLifecycleRuleArgs(
            action=gcp.storage.BucketLifecycleRuleActionArgs(
                type='AbortIncompleteMultipartUpload',
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
        project: Optional[str] = None,
        soft_delete_protection: bool = True,
    ) -> Any:
        unique_bucket_name = name
        if not unique:
            unique_bucket_name = (
                f'{self.config.gcp.dataset_storage_prefix}{self.dataset}-{name}'
            )

        def autoclass_args():
            # Only set the parameter if required, to avoid superflous changes to existing buckets.
            return gcp.storage.BucketAutoclassArgs(enabled=True) if autoclass else None

        def soft_delete_policy_args():
            # If enabled, use None to let GCP use the default policy
            if soft_delete_protection:
                return None

            # If soft delete protection is disabled, set retention to 0 to disable it
            return gcp.storage.BucketSoftDeletePolicyArgs(retention_duration_seconds=0)

        return gcp.storage.Bucket(
            self.get_pulumi_name(name + '-bucket'),
            name=unique_bucket_name,
            location=self.region,
            uniform_bucket_level_access=True,
            versioning=gcp.storage.BucketVersioningArgs(enabled=versioning),
            autoclass=autoclass_args(),
            soft_delete_policy=soft_delete_policy_args(),
            labels={'bucket': unique_bucket_name},
            # duplicate the array to avoid adding the lifecycle rule to an existing list
            lifecycle_rules=[
                *lifecycle_rules,
                self.bucket_rule_abort_incomplete_multipart_upload(),
            ],
            requester_pays=requester_pays,
            project=project or self.project_id,
        )

    def get_group_key(self, group) -> str | pulumi.Output[str]:
        if isinstance(group, (gcp.serviceaccount.Account, gcp.storage.Bucket)):
            raise ValueError(f'Incorrect type for group key: {type(group)}')

        if isinstance(group, gcp.cloudidentity.Group):
            return group.id
            # return pulumi.Output.concat('group:', group.group_key.id)

        if isinstance(group, str):
            assert self.config.gcp
            if group.endswith(
                '@' + self.config.gcp.groups_domain,
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

    # This method returns a list so that changes to roles can be made in a safe way by
    # adding a new role without deleting the old one. The resource key is defined
    # explicitly so that resources can maintain the same resource key no matter where
    # they appear in the list.
    def bucket_membership_to_role_list(
        self,
        membership: BucketMembership,
        resource_key: str,
    ):
        if membership == BucketMembership.MUTATE:
            return [
                # This role allows mutation of bucket objects but not deletion of the
                # bucket itself
                BucketMembershipRole(
                    f'{self.organization.id}/roles/StorageObjectAndBucketMutator',
                    f'{resource_key}-no-bucket-deletion',
                ),
            ]
        if membership == BucketMembership.APPEND:
            return [
                BucketMembershipRole(
                    f'{self.organization.id}/roles/StorageViewerAndCreator',
                    resource_key,
                ),
            ]
        if membership == BucketMembership.READ:
            return [
                BucketMembershipRole(
                    f'{self.organization.id}/roles/StorageObjectAndBucketViewer',
                    resource_key,
                ),
            ]
        if membership == BucketMembership.LIST:
            return [
                BucketMembershipRole(
                    f'{self.organization.id}/roles/StorageLister',
                    resource_key,
                ),
            ]

        raise ValueError(f'Unrecognised bucket membership type {membership}')

    def add_member_to_bucket(
        self,
        resource_key: str,
        bucket,
        member,
        membership: BucketMembership,
    ) -> Any:
        role_list = self.bucket_membership_to_role_list(membership, resource_key)

        for role_item in role_list:
            gcp.storage.BucketIAMMember(
                self.get_pulumi_name(role_item.resource_key),
                bucket=get_member_key(bucket),
                member=get_member_key(member),
                role=role_item.role,
                opts=pulumi.resource.ResourceOptions(
                    depends_on=[self._svc_cloudidentity],
                ),
            )

    def give_member_ability_to_list_buckets(
        self,
        resource_key: str,
        member,
        project: Optional[str] = None,
    ):
        role_list = self.bucket_membership_to_role_list(
            BucketMembership.LIST,
            resource_key,
        )

        for role_item in role_list:
            gcp.projects.IAMMember(
                self.get_pulumi_name(role_item.resource_key),
                role=role_item.role,
                member=get_member_key(member),
                project=project or self.project_id,
                opts=pulumi.resource.ResourceOptions(
                    depends_on=[self._svc_cloudidentity],
                ),
            )

    def create_machine_account(
        self,
        name: str,
        project: Optional[str] = None,
        *,
        resource_key: Optional[str] = None,
    ) -> Any:
        if project and isinstance(project, gcp.organizations.Project):
            project = project.project_id

        return gcp.serviceaccount.Account(
            self.get_pulumi_name(resource_key or f'service-account-{name}'),
            account_id=name,
            create_ignore_already_exists=True,
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
            member=get_member_key(member),
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudidentity, self._svc_iam],
            ),
        )

    def get_credentials_for_machine_account(self, resource_key, account):
        return gcp.serviceaccount.Key(
            self.get_pulumi_name(resource_key),
            service_account_id=account.email,
        ).private_key.apply(lambda s: base64.b64decode(s).decode('utf-8'))

    def create_group(self, name: str) -> Any:
        mail = f'{name}@{self.config.gcp.groups_domain}'

        # Dev GCP accounts don't have access to create empty groups, so on dev they are
        # created with the initial owner
        initial_group_config = (
            'EMPTY' if self.config.gcp.create_empty_groups else 'WITH_INITIAL_OWNER'
        )

        group = gcp.cloudidentity.Group(
            self.get_pulumi_name(name + '-group'),
            display_name=name,
            initial_group_config=initial_group_config,
            group_key=gcp.cloudidentity.GroupGroupKeyArgs(id=mail),
            labels={'cloudidentity.googleapis.com/groups.discussion_forum': ''},
            parent=f'customers/{self.config.gcp.customer_id}',
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

        # Only set allowExternalMembers': 'true' if settings specify it
        # this defaults to True
        if self.config.gcp.allow_external_group_members:
            # Allow domain-external members in the group.
            GoogleGroupSettings(
                self.get_pulumi_name(name + '-group-settings'),
                group_email=mail,
                settings={'allowExternalMembers': 'true'},
                opts=pulumi.resource.ResourceOptions(depends_on=[group]),
            )
        return group

    def add_group_member(
        self,
        resource_key: str,
        group,
        member,
        unique_resource_key: bool = False,
    ) -> Any:
        if self.config.disable_group_memberships:
            return

        if not unique_resource_key:
            resource_key = self.get_pulumi_name(resource_key)

        GoogleGroupMembership(
            resource_key,
            props=GoogleGroupMembershipInputs(
                group_key=self.get_group_key(group),
                member_key=get_preferred_group_membership_key(member),
            ),
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def create_secret(
        self,
        name: str,
        project: Optional[str] = None,
        resource_key: Optional[str] = None,
    ) -> Any:
        return gcp.secretmanager.Secret(
            resource_key or self.get_pulumi_name(name),
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
        project: Optional[str] = None,
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
            if secret.count('/') != 3:  # noqa: PLR2004
                if isinstance(project, gcp.organizations.Project):
                    project = project.id
                secret_id = pulumi.Output.concat(
                    'projects/',
                    project or self.project_id,
                    '/secrets/',
                    secret,
                )
        else:
            raise ValueError(f'Unexpected secret type: {secret} ({type(secret)})')

        gcp.secretmanager.SecretIamMember(
            self.get_pulumi_name(resource_key),
            project=project or self.project_id,
            secret_id=secret_id,
            role=role,
            member=get_member_key(member),
        )

    def add_secret_version(
        self,
        resource_key: str,
        secret: Any,
        contents: Any,
    ):
        return gcp.secretmanager.SecretVersion(
            self.get_pulumi_name(resource_key),
            secret=secret.id,
            secret_data=contents,
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
        self,
        resource_key: str,
        registry,
        member,
        membership,
        project=None,
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
            member=get_member_key(member),
        )

    # endregion CONTAINER REGISTRY

    # region GCP SPECIFIC

    def add_member_to_batch_api(self, resource_key: str, account):
        gcp.projects.IAMMember(
            self.get_pulumi_name(resource_key),
            role='roles/batch.agentReporter',
            member=get_member_key(account),
            project=self.project_id,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_batchapi]),
        )

    def add_member_to_dataproc_api(self, resource_key: str, account, role: str):
        if role in ('worker', 'admin'):
            role = f'roles/dataproc.{role}'

        gcp.projects.IAMMember(
            self.get_pulumi_name(resource_key),
            role=role,
            member=get_member_key(account),
            project=self.project_id,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_dataproc]),
        )

    def add_cloudrun_invoker(
        self,
        resource_key: str,
        *,
        service: str,
        project: str,
        member,
    ):
        gcp.cloudrun.IamMember(
            self.get_pulumi_name(resource_key),
            location=self.region,
            project=project or self.project_id,
            service=service,
            role='roles/run.invoker',
            member=get_member_key(member),
        )

    def add_project_role(
        self,
        resource_key: str,
        *,
        member: Any,
        role: str,
        project: Optional[str] = None,
    ):
        gcp.projects.IAMMember(
            self.get_pulumi_name(resource_key),
            project=project or self.project_id,
            role=role,
            member=get_member_key(member),
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
