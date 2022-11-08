# pylint: disable=missing-class-docstring, missing-function-docstring
"""
AWS implementation for abstract infrastructure
"""
from datetime import date
from functools import cached_property
from typing import Any

import pulumi_aws_native as aws_native
import pulumi_aws as aws

from cpg_infra.abstraction.base import (
    CloudInfraBase,
    UNDELETE_PERIOD_IN_DAYS,
    TMP_BUCKET_PERIOD_IN_DAYS,
    SecretMembership,
    ContainerRegistryMembership,
    BucketMembership,
    ARCHIVE_PERIOD_IN_DAYS,
)
from cpg_infra.config import CPGInfrastructureConfig, CPGDatasetConfig


class AWSInfra(CloudInfraBase):
    @staticmethod
    def name():
        return 'aws'

    def get_dataset_project_id(self):
        return self.dataset

    def create_project(self, name):
        return name

    def create_monthly_budget(self, resource_key: str, *, project, budget):
        pass

    def create_fixed_budget(
        self, resource_key: str, *, project, budget, start_date: date = date(2022, 1, 1)
    ):
        pass

    def bucket_rule_undelete(self, days=UNDELETE_PERIOD_IN_DAYS) -> Any:
        return aws.s3.BucketLifecycleRuleArgs(
            enabled=True,
            noncurrent_version_expiration=aws.s3.BucketLifecycleRuleNoncurrentVersionExpiration(days=days),
        )

    def bucket_rule_temporary(self, days=TMP_BUCKET_PERIOD_IN_DAYS) -> Any:
        # TODO: https://www.pulumi.com/registry/packages/aws/api-docs/s3/bucket/#bucketrule
        return aws.s3.BucketLifecycleRuleArgs(
                enabled=True,
                expiration=aws.s3.BucketLifecycleRuleExpirationArgs(
                    days=TMP_BUCKET_PERIOD_IN_DAYS,
                ),
                id="tmp",
            )

    def bucket_rule_archive(self, days=ARCHIVE_PERIOD_IN_DAYS) -> Any:
        pass

    def create_bucket(
        self,
        name: str,
        lifecycle_rules: list,
        unique: bool = False,
        requester_pays: bool = False,
        versioning: bool = True,
        project: str = None,
    ) -> Any:
        unique_bucket_name = name
        if not unique:
            unique_bucket_name = (
                f'{self.config.dataset_storage_prefix}{self.dataset}-{name}'
            )
        return aws.s3.Bucket(
            unique_bucket_name,
            bucket=unique_bucket_name,
            lifecycle_rules=lifecycle_rules,
            request_payer='Requester' if requester_pays else 'BucketOwner',
            tags={
                'dataset': project or self.dataset
            }
        )

    def add_member_to_bucket(
        self, resource_key: str, bucket, member, membership: BucketMembership
    ) -> Any:
        pass

    def give_member_ability_to_list_buckets(
        self, resource_key: str, member, project: str = None
    ):
        pass

    def create_machine_account(
        self, name: str, project: str = None, *, resource_key: str = None
    ) -> Any:
        pass

    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member
    ) -> Any:
        pass

    def get_credentials_for_machine_account(self, resource_key, account):
        pass

    def create_group(self, name: str) -> Any:
        return aws.iam.Group(name, path='/users/')

    def add_group_member(self, resource_key: str, group, member) -> Any:
        return aws.iam.GroupMembership(
            resource_key, group=group.name, users=[member.name]
        )

    def create_secret(self, name: str, project: str = None) -> Any:
        secret = aws.secretsmanager.Secret(name, name=name, tags={
                'dataset': project or self.dataset
            })

        return secret

    def add_secret_member(
        self,
        resource_key: str,
        secret,
        member,
        membership: SecretMembership,
        project: str = None,
    ) -> Any:
        pass

    def add_secret_version(self, resource_key: str, secret: Any, contents: Any):
        return aws.secretsmanager.SecretVersion(
            resource_key, secret_id=secret.id, secret_string=contents
        )

    def add_member_to_container_registry(
        self,
        resource_key: str,
        registry,
        member,
        membership: ContainerRegistryMembership,
        project: str = None,
    ) -> Any:
        pass
