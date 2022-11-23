# pylint: disable=missing-class-docstring, missing-function-docstring
"""
AWS implementation for abstract infrastructure
"""
from collections import defaultdict
from datetime import date
from functools import cached_property
from typing import Any

import pulumi

# import pulumi_aws_native as aws_native
import pulumi_aws as aws

from cpg_infra.abstraction.base import (
    CloudInfraBase,
    UNDELETE_PERIOD_IN_DAYS,
    TMP_BUCKET_PERIOD_IN_DAYS,
    SecretMembership,
    ContainerRegistryMembership,
    BucketMembership,
    ARCHIVE_PERIOD_IN_DAYS,
    group_by,
)
from cpg_infra.config import CPGInfrastructureConfig, CPGDatasetConfig


class AWSInfra(CloudInfraBase):
    """AWS implementation for cloud abstraction"""

    def __init__(self, config, dataset_config):
        super().__init__(config=config, dataset_config=dataset_config)

        self.group_memberships = defaultdict(dict)
        self.entity_to_name: dict[any, str] = {}  # used for policy naming
        self.iam_bucket_memberships: dict[any, dict[aws.s3.Bucket, list[BucketMembership]]] = defaultdict(lambda: defaultdict(list))

    @staticmethod
    def name():
        return 'aws'

    def get_dataset_project_id(self):
        return self.dataset

    def create_project(self, name):
        return name

    def bucket_output_path(self, bucket):
        return pulumi.Output.concat('s3://', bucket.bucket)

    @staticmethod
    def get_identifier(obj):
        if isinstance(obj, str):
            return obj
        if isinstance(obj, pulumi.Output):
            return obj
        if isinstance(obj, aws.iam.Group):
            return obj.name
        if isinstance(obj, aws.iam.User):
            return obj.id

        raise ValueError(f'Unrecognised type {obj}: {type(obj)}')

    def finalise(self):
        """Finalize AWS creation"""
        self.finalise_group_memberships()
        self.finalise_bucket_memberships()

    def finalise_group_memberships(self):
        for group, members in self.group_memberships.items():
            for resource_key, member in members.items():
                if isinstance(member, aws.iam.Group):
                    # User groups can't be nested; they can contain only users, not other user groups.
                    # https://docs.aws.amazon.com/IAM/latest/UserGuide/id_groups.html
                    continue

                aws.iam.GroupMembership(
                    self.resource_prefix() + resource_key,
                    group=group.name,
                    users=[member.name],
                )

    @staticmethod
    def bucket_membership_to_s3_policy(membership: BucketMembership):
        if membership == BucketMembership.LIST:
            return ['s3:ListBucket']
        if membership == BucketMembership.READ:
            return ['s3:ListBucket', 's3:GetObject']
        if membership == BucketMembership.APPEND:
            return [
                's3:ListBucket',
                's3:GetObject',
                's3:GetObjectVersion',
                's3:AbortMultipartUpload',
                's3:PutObject',
            ]
        if membership == BucketMembership.MUTATE:
            return [
                's3:ListBucket',
                's3:GetObject',
                's3:DeleteObject',
                's3:DeleteObjectVersion',
                's3:AbortMultipartUpload',
                's3:PutObject',
                's3:RestoreObject',
            ]

        raise NotImplementedError(f'Unhandled bucket membership: {membership}')

    def finalise_bucket_memberships(self):

        # this feels gross as, and I can't actually work out how to do it:
        # - policy is too long, do I split it up into different policies
        # - referencing entity from dict[entity: name] doesn't work

        statements = []
        for entity, entities in self.iam_bucket_memberships.items():
            for bucket, memberships in entities.items():
                actions = set(role for m in memberships for role in self.bucket_membership_to_s3_policy(m))

                statements.append(
                        aws.iam.GetPolicyDocumentStatementArgs(
                            actions=list(actions),
                            resources=[
                                bucket.arn,
                                bucket.arn.apply(lambda arn: f"{arn}/*"),
                            ]
                        )
                )

            policy = aws.iam.get_policy_document_output(statements=statements)
            # policy.json.apply(lambda value: print(f'{self.entity_to_name[entity]}-policy: ' + value))
            # if isinstance(entity, aws.iam.Group):
            #     aws.iam.GroupPolicy(
            #         self.resource_prefix() + f'{self.entity_to_name[entity]}-bucket-policy',
            #         name=f'{self.entity_to_name[entity]}-policy',
            #         group=entity.name,
            #         policy=policy.json,
            #     )

    @staticmethod
    def resource_prefix():
        return 'aws-'

    def get_tags(self, project=None):
        return {'dataset': project or self.dataset}

    def create_monthly_budget(self, resource_key: str, *, project, budget):
        return self._create_budget(
            resource_key,
            time_period_end="2087-06-15_00:00",
            time_period_start="2017-07-01_00:00",
            time_unit="MONTHLY",
            project=project,
            budget=budget,
        )

    def create_fixed_budget(
        self, resource_key: str, *, project, budget, start_date: date = date(2022, 1, 1)
    ):
        return self._create_budget(
            resource_key,
            time_period_end="2087-06-15_00:00",
            time_period_start="2017-07-01_00:00",
            # time_unit="MONTHLY",
            project=project,
            budget=budget,
        )

    def _create_budget(self, resource_key, budget, project, **kwargs):
        if self.config.aws and self.config.aws.subscriber_sns_topic_arns:

            notifications = [
                aws.budgets.BudgetNotification(
                    threshold=threshold,
                    threshold_type='PERCENT',
                    notification_type='ACTUAL',
                    comparison_operator="GREATER_THAN",
                )
                for threshold in self.config.budget_notification_thresholds
            ]

            notifications.append(
                aws.budgets.BudgetNotification(
                    treshold='100',
                    threshold_type='PERCENT',
                    comparison_operator="GREATER_THAN",
                    notification_type='ACTUAL',
                    subscriber_sns_topic_arns=self.config.aws.subscriber_sns_topic_arns,
                )
            )

            kwargs['notifications'] = kwargs['notification']

        return aws.budgets.Budget(
            self.resource_prefix() + resource_key,
            budget_type="COST",
            cost_filters=[
                aws.budgets.BudgetCostFilterArgs(
                    name="Tag",
                    values=[project if isinstance(project, str) else project.name],
                )
            ],
            limit_amount=str(budget),
            limit_unit=self.config.budget_currency,
            **kwargs,
        )

    def bucket_rule_undelete(self, days=UNDELETE_PERIOD_IN_DAYS) -> Any:
        return aws.s3.BucketLifecycleRuleArgs(
            enabled=True,
            noncurrent_version_expiration=aws.s3.BucketLifecycleRuleNoncurrentVersionExpirationArgs(
                days=days
            ),
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

        # todo: remove this when lifecycle rules are correct
        lifecycle_rules = [l for l in lifecycle_rules if l]

        bucket = aws.s3.Bucket(
            self.resource_prefix() + unique_bucket_name,
            bucket=unique_bucket_name,
            lifecycle_rules=lifecycle_rules,
            request_payer='Requester' if requester_pays else 'BucketOwner',
            tags=self.get_tags(project),
        )
        self.entity_to_name[bucket] = unique_bucket_name

        return bucket

    def add_member_to_bucket(
        self, resource_key: str, bucket, member, membership: BucketMembership
    ) -> Any:
        self.iam_bucket_memberships[member][bucket].append(membership)

    def give_member_ability_to_list_buckets(
        self, resource_key: str, member, project: str = None
    ):
        pass

    def create_machine_account(
        self, name: str, project: str = None, *, resource_key: str = None
    ) -> Any:
        machine_account = aws.iam.User(
            self.resource_prefix() + 'machine-account-' + name,
            path='/system/',
            name=self.dataset + '-' + name,
            tags=self.get_tags(project or self.dataset),
        )
        self.entity_to_name[machine_account] = name
        return machine_account

    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member, project: str = None
    ) -> Any:
        pass

    def get_credentials_for_machine_account(self, resource_key, account):
        return aws.iam.AccessKey(
            self.resource_prefix() + resource_key, user=account.name
        )

    def create_group(self, name: str) -> Any:
        group = aws.iam.Group(
            self.resource_prefix() + 'group-' + name,
            name=self.config.dataset_storage_prefix + name,
            path='/users/',
        )
        self.entity_to_name[group] = name
        return group

    def add_group_member(self, resource_key: str, group, member) -> Any:
        self.group_memberships[group][resource_key] = member

    def create_secret(self, name: str, project: str = None) -> Any:
        secret = aws.secretsmanager.Secret(
            self.resource_prefix() + 'secret-' + name,
            name=name,
            tags=self.get_tags(project),
        )

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
            self.resource_prefix() + resource_key,
            secret_id=secret.id,
            secret_string=contents,
        )

    def create_container_registry(self, name: str):
        return aws.ecr.Repository(
            self.resource_prefix() + 'container-registry-' + name,
            name=name,
            tags=self.get_tags(),
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

    def add_blob_to_bucket(self, resource_name, bucket, output_name, contents):
        return aws.s3.BucketObject(
            self.resource_prefix() + resource_name,
            bucket=bucket,
            key=output_name,
            content=contents,
        )
