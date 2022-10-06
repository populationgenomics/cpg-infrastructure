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
)
from cpg_infra.config import CPGDatasetConfig, CPGInfrastructureConfig


class GcpInfrastructure(CloudInfraBase):
    @staticmethod
    def name():
        return 'gcp'

    def __init__(
        self, config: CPGInfrastructureConfig, dataset_config: CPGDatasetConfig
    ):
        super().__init__(config, dataset_config)

        self.region = 'australia-southeast1'
        self.organization = gcp.organizations.get_organization(domain=config.domain)
        self.project_id = gcp.organizations.get_project().project_id

        self._svc_cloudresourcemanager = gcp.projects.Service(
            'cloudresourcemanager-service',
            service='cloudresourcemanager.googleapis.com',
            disable_on_destroy=False,
        )

        self._svc_cloudidentity = gcp.projects.Service(
            'cloudidentity-service',
            service='cloudidentity.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
        )

        self._svc_serviceusage = gcp.projects.Service(
            'serviceusage-service',
            service='serviceusage.googleapis.com',
            disable_on_destroy=False,
        )

        self._svc_secretmanager = gcp.projects.Service(
            'secretmanager-service',
            service='secretmanager.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
        )

    def get_dataset_project_id(self):
        return self.project_id

    # region SERVICES
    @cached_property
    def _svc_dataproc(self):
        return gcp.projects.Service(
            'dataproc-service',
            service='dataproc.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
        )

    @cached_property
    def _svc_lifescienceapi(self):
        return gcp.projects.Service(
            'lifesciences-service',
            service='lifesciences.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_serviceusage]),
        )

    @cached_property
    def _svc_cloudbilling(self):
        return gcp.projects.Service(
            'cloudbilling-service',
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
            'cloudbillingbudgets-service',
            service='billingbudgets.googleapis.com',
            disable_on_destroy=False,
            project=self.project_id,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudbilling]),
        )

    @cached_property
    def _svc_iam(self):
        return gcp.projects.Service(
            'iam-service',
            service='iam.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
        )

    # endregion SERVICES

    def create_project(self, name):
        return gcp.organizations.Project(
            f'{name}-project',
            org_id=self.organization.org_id,
            project_id=name,
            name=name,
            billing_account=self.config.gcp.billing_account_id,
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
            resource_key,
            amount=gcp.billing.BudgetAmountArgs(
                specified_amount=gcp.billing.BudgetAmountSpecifiedAmountArgs(
                    units=str(budget),
                    currency_code=self.config.budget_currency,
                    nanos=0,
                )
            ),
            billing_account=self.config.gcp.billing_account_id,
            display_name=project.name,
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
        return self.create_budget(
            resource_key=resource_key,
            project=project,
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
        )

    def create_monthly_budget(self, resource_key: str, *, project, budget: int):
        return self.create_budget(
            resource_key=resource_key,
            project=project,
            budget=budget,
            budget_filter=gcp.billing.BudgetBudgetFilterArgs(
                projects=[pulumi.Output.concat('projects/', project.number)],
                calendar_period='month',
            ),
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

    def create_bucket(
        self,
        name: str,
        lifecycle_rules: list,
        unique=False,
        requester_pays=False,
        versioning: bool = True,
        project: str = None,
    ) -> Any:
        unique_bucket_name = name
        if not unique:
            unique_bucket_name = (
                f'{self.config.dataset_storage_prefix}{self.dataset}-{name}'
            )
        return gcp.storage.Bucket(
            unique_bucket_name,
            name=unique_bucket_name,
            location=self.region,
            uniform_bucket_level_access=True,
            versioning=gcp.storage.BucketVersioningArgs(enabled=versioning),
            labels={'bucket': unique_bucket_name},
            lifecycle_rules=lifecycle_rules,
            requester_pays=requester_pays,
            project=project or self.project_id,
        )

    def get_member_key(self, member):
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
            if group.endswith('@populationgenomics.org.au') and not group.startswith(
                'group:'
            ):
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
            resource_key,
            bucket=self.get_member_key(bucket),
            member=self.get_member_key(member),
            role=self.bucket_membership_to_role(membership),
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def give_member_ability_to_list_buckets(
        self, resource_key: str, member, project: str = None
    ):
        gcp.projects.IAMMember(
            resource_key,
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
            resource_key or f'service-account-{name}',
            account_id=name,
            # display_name=name,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_iam]),
            project=project,
        )

    # pylint: disable=unused-argument
    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member, project: str = None
    ) -> Any:
        # TODO: action project here
        gcp.serviceaccount.IAMMember(
            resource_key,
            service_account_id=machine_account.name,
            role='roles/iam.serviceAccountUser',
            member=self.get_member_key(member),
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudidentity, self._svc_iam]
            ),
        )

    def get_credentials_for_machine_account(self, resource_key, account):
        return gcp.serviceaccount.Key(
            resource_key,
            service_account_id=account.email,
        ).private_key.apply(lambda s: base64.b64decode(s).decode('utf-8'))

    def create_group(self, name: str) -> Any:
        mail = f'{name}@populationgenomics.org.au'
        return gcp.cloudidentity.Group(
            name,
            display_name=name,
            group_key=gcp.cloudidentity.GroupGroupKeyArgs(id=mail),
            labels={'cloudidentity.googleapis.com/groups.discussion_forum': ''},
            parent=f'customers/{self.config.gcp.customer_id}',
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def add_group_member(self, resource_key: str, group, member) -> Any:
        if self.config.disable_group_memberships:
            return

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
            name,
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

        gcp.secretmanager.SecretIamMember(
            resource_key,
            project=project or self.project_id,
            secret_id=secret.id,
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
            resource_key, secret=secret.id, secret_data=contents
        )

    def add_member_to_container_registry(
        self, resource_key: str, registry, member, membership, project=None
    ) -> Any:

        if membership == ContainerRegistryMembership.READER:
            role = 'roles/artifactregistry.reader'
        elif membership == ContainerRegistryMembership.APPEND:
            role = 'roles/artifactregistry.writer'
        else:
            raise ValueError(f'Unrecognised group membership type: {membership}')

        gcp.artifactregistry.RepositoryIamMember(
            resource_key,
            project=project or self.project_id,
            location=self.region,
            repository=registry,
            role=role,
            member=self.get_member_key(member),
        )

    # region GCP SPECIFIC

    def add_member_to_lifescience_api(self, resource_key: str, account):
        gcp.projects.IAMMember(
            resource_key,
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
            resource_key,
            role=role,
            member=self.get_member_key(account),
            project=self.project_id,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_dataproc]),
        )

    def add_cloudrun_invoker(
        self, resource_key: str, *, service: str, project: str, member
    ):
        gcp.cloudrun.IamMember(
            resource_key,
            location=self.region,
            project=project,
            service=service,
            role='roles/run.invoker',
            member=self.get_member_key(member),
        )

    def add_project_role(
        self, resource_key: str, *, member: Any, role: str, project: str = None
    ):
        gcp.projects.IAMMember(
            resource_key,
            project=project or self.project_id,
            role=role,
            member=self.get_member_key(member),
        )

    # endregion GCP SPECIFIC
