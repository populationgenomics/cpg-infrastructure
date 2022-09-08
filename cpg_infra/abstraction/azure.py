from functools import lru_cache
from typing import Any

import pulumi_azure_native as az

from cpg_infra.abstraction.base import (
    CloudInfraBase,
    UNDELETE_PERIOD_IN_DAYS,
    ARCHIVE_PERIOD_IN_DAYS,
    TMP_BUCKET_PERIOD_IN_DAYS,
    SecretMembership,
)
from cpg_infra.config import CPGInfrastructureConfig, CPGDatasetConfig


class AzureInfra(CloudInfraBase):
    def __init__(
        self, config: CPGInfrastructureConfig, dataset_config: CPGDatasetConfig
    ):
        super().__init__(config, dataset_config)

        self.resource_group_name = f'{config.dataset_storage_prefix}{self.dataset}'
        self.storage_account_name = f'{config.dataset_storage_prefix}{self.dataset}'

    @staticmethod
    def name():
        return 'azure'

    @property
    @lru_cache()
    def subscription(self):
        return az.subscription.Subscription(self.dataset)

    @property
    @lru_cache()
    def resource_group(self):
        return az.resources.ResourceGroup('resource_group', subscription=self.subscription)

    @property
    @lru_cache()
    def storage_account(self):
        return az.storage.Account(
            self.storage_account_name, resource_group=self.resource_group
        )

    def bucket_rule_undelete(self, days=UNDELETE_PERIOD_IN_DAYS) -> Any:
        return az.storage.BucketLifecycleRuleArgs(
            action=az.storage.BucketLifecycleRuleActionArgs(type='Delete'),
            condition=az.storage.BucketLifecycleRuleConditionArgs(
                days_since_noncurrent_time=days, with_state='ARCHIVED'
            ),
        )

    def bucket_rule_archive(self, days=ARCHIVE_PERIOD_IN_DAYS) -> Any:
        return az.storage.BucketLifecycleRuleArgs(
            action=az.storage.BucketLifecycleRuleActionArgs(
                type='SetStorageClass', storage_class='ARCHIVE'
            ),
            condition=az.storage.BucketLifecycleRuleConditionArgs(age=days),
        )

    def bucket_rule_temporary(self, days=TMP_BUCKET_PERIOD_IN_DAYS) -> Any:
        return az.storage.BucketLifecycleRuleArgs(
            action=az.storage.BucketLifecycleRuleActionArgs(type='Delete'),
            condition=az.storage.BucketLifecycleRuleConditionArgs(age=days),
        )

    def create_bucket(
        self,
        name: str,
        lifecycle_rules: list,
        unique: bool = False,
        requester_pays: bool = False,
        versioning: bool = True,
        project: str = None,
    ) -> Any:
        return az.storage.BlobContainer(
            f'{name}',
            location=self.region,
            uniform_bucket_level_access=True,
            versioning=az.storage.BucketVersioningArgs(enabled=versioning),
            labels={'bucket': name},
            lifecycle_rules=lifecycle_rules,
            requester_pays=requester_pays,
            project=project or self.project,
            resource_group_name=self.resource_group_name,
            account_name=self.storage_account_name,
            container_name=name,
        )

    def add_member_to_bucket(
        self, resource_key: str, bucket, member, membership
    ) -> Any:
        az.authorization.RoleAssignment(
            resource_key,
            scope=bucket.id,
            principal_id=member.id,
            role_definition_id='Contributor',
            role_assignment_name='Storage Blob Data Contributor',
        )

    def create_machine_account(
        self, name: str, project: str = None, *, resource_key: str = None
    ) -> Any:
        application = az.batch.Application(
            f'application-{name}',
            account_name=name,
            display_name=name,
            resource_group_name=self.resource_group_name,
        )
        return application

    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member
    ) -> Any:
        pass

    def create_group(self, name: str) -> Any:
        pass

    def add_group_member(self, resource_key: str, group, member) -> Any:
        pass

    def create_secret(self, name: str, project: str = None) -> Any:
        pass

    def add_secret_member(
        self,
        resource_key: str,
        secret,
        member,
        membership: SecretMembership,
        project: str = None,
    ) -> Any:
        pass

    def add_member_to_container_registry(
        self, resource_key: str, registry, member, membership, project=None
    ) -> Any:
        pass
