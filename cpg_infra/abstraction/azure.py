# pylint: disable=missing-class-docstring, missing-function-docstring
"""
Azure implementation for abstract infrastructure
"""
from datetime import date

from typing import Any, Callable
from functools import cached_property

import pulumi_azure_native as az

from cpg_infra.config import CPGInfrastructureConfig, CPGDatasetConfig
from cpg_infra.abstraction.base import (
    CloudInfraBase,
    UNDELETE_PERIOD_IN_DAYS,
    ARCHIVE_PERIOD_IN_DAYS,
    TMP_BUCKET_PERIOD_IN_DAYS,
    SecretMembership,
)

AZURE_BILLING_START_DATE = "2017-06-01T00:00:00Z"
AZURE_BILLING_EXPIRY_DATE = "3141-25-09T00:00:00Z"
class AzureInfra(CloudInfraBase):
    def __init__(
        self, config: CPGInfrastructureConfig, dataset_config: CPGDatasetConfig
    ):
        super().__init__(config, dataset_config)

        self.region = 'australiaeast'
        self.subscription = config.azure.subscription_id
        self._resource_group_name = f'{config.dataset_storage_prefix}{self.dataset}'
        self._storage_account_name = f'{config.dataset_storage_prefix}{self.dataset}'

    @staticmethod
    def name():
        return 'azure'

    def get_dataset_project_id(self):
        return self.dataset

    @cached_property
    def resource_group(self):
        return az.resources.ResourceGroup(
            self._resource_group_name,
            location=self.region
        )

    @cached_property
    def storage_account(self):
        return az.storage.StorageAccount(
            self._storage_account_name,
            resource_group_name=self.resource_group.name,
            kind="StorageV2",
            sku=az.storage.SkuArgs(name="Standard_LRS")
        )

    def _sanatize(self, name):
        return ''.join(word.title() for word in name.split('-'))

    def create_project(self, name):
        # TODO: re-work creating shared projects in Azure
        return az.resources.ResourceGroup(name)

    def create_budget(self, resource_key: str, *, project, budget: int, budget_filter: az.consumption.BudgetArgs):
        kwargs = {}
        # TODO: setup Azure notifications for budget rules
        # if self.config.gcp.budget_notification_pubsub:
        #     kwargs['threshold_rules'] = [
        #         gcp.billing.BudgetThresholdRuleArgs(threshold_percent=threshold)
        #         for threshold in self.config.budget_notification_thresholds
        #     ]
        #     kwargs['all_updates_rule'] = gcp.billing.BudgetAllUpdatesRuleArgs(
        #         pubsub_topic=self.config.gcp.budget_notification_pubsub,
        #         schema_version='1.0',
        #     )

        filters = budget_filter.pop('filter')
        kwargs = dict(kwargs, dict(budget_filter))
        
        az.consumption.Budget(
            resource_key,
            budget_name=f'{project.name}-budget',
            amount=budget,
            category="Cost",
            scope=f"/subscriptions/{self.subscription}/",
            budget_filter=filters,
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
        filters=az.consumption.BudgetFilterArgs(
            and_=[
                az.consumption.BudgetFilterPropertiesArgs(
                    dimensions=az.consumption.BudgetComparisonExpressionArgs(
                        name="ResourceId",
                        operator="In",
                        values=[project.id],
                    ),
                )
            ]
        )
        return self.create_budget(
            resource_key=resource_key,
            project=project,
            budget=budget,
            budget_filter=az.consumption.BudgetArgs(
                time_grain= 'Annually',
                time_period= az.consumption.BudgetTimePeriodArgs(
                    start_date=str(start_date),
                    end_date= AZURE_BILLING_EXPIRY_DATE
                ),
                filter=filters
            )
        )

    def create_monthly_budget(self, resource_key: str, *, project, budget: int):
        # No start date here thats an issue
        filters=az.consumption.BudgetFilterArgs(
            and_=[
                az.consumption.BudgetFilterPropertiesArgs(
                    dimensions=az.consumption.BudgetComparisonExpressionArgs(
                        name="ResourceId",
                        operator="In",
                        values=[project.id],
                    ),
                )
            ]
        )
        return self.create_budget(
            resource_key=resource_key,
            project=project,
            budget=budget,
            budget_filter=az.consumption.BudgetArgs(
                time_grain='Monthly',
                time_period=az.consumption.BudgetTimePeriodArgs(
                    start_date=AZURE_BILLING_START_DATE,
                    end_date=AZURE_BILLING_EXPIRY_DATE
                ),
                filter=filters
            )
        )

    def bucket_rule_undelete(self, days=UNDELETE_PERIOD_IN_DAYS) -> Any:
        az.storage.BlobServiceProperties(
            "storage_account_undelete_rule",
            account_name=self.storage_account.name,
            blob_services_name="default",
            delete_retention_policy=az.storage.DeleteRetentionPolicyArgs(days=days, enabled=True),
            resource_group_name=self.resource_group.name
        )
        return None

    def bucket_rule_archive(self, days=ARCHIVE_PERIOD_IN_DAYS) -> Any:
        return az.storage.ManagementPolicyRuleArgs(
            name='bucket-rule-archive',
            type='LifeCycle',
            definition=az.storage.ManagementPolicyDefinitionArgs(
                actions=az.storage.ManagementPolicyActionArgs(
                    base_blob=az.storage.ManagementPolicyBaseBlobArgs(
                        tier_to_archive=az.storage.DateAfterModificationArgs(
                            days_after_modification_greater_than=days,
                        )
                    )
                )
            ),
        )

    def bucket_rule_temporary(self, days=TMP_BUCKET_PERIOD_IN_DAYS) -> Any:
        return az.storage.ManagementPolicyRuleArgs(
            name='bucket-rule-tmp',
            type='LifeCycle',
            definition=az.storage.ManagementPolicyDefinitionArgs(
                actions=az.storage.ManagementPolicyActionArgs(
                    base_blob=az.storage.ManagementPolicyBaseBlobArgs(
                        delete=az.storage.DateAfterModificationArgs(
                            days_after_modification_greater_than=days,
                        ),
                    )
                )
            ),
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
        # Policies are set at the storage account level
        # the rules are generated using other function calls
        # e.g. bucket_rule_archive
        # They are then passed here to apply to a particular 'bucket'
        # (azure blob container)
        # So first we modify to filter the rule to apply only to the
        # new bucket
        bucket_filter = az.storage.ManagementPolicyFilterArgs(
            prefix_match=name,
            blob_types=["blockBlob"]
        )

        def apply_filter(rule):
            rule.definition.filters = bucket_filter
            return rule

        lifecycle_rules = filter(lambda x: x, lifecycle_rules)
        lifecycle_rules = map(apply_filter, lifecycle_rules)

        # Set storage account versioning and lifecycle rules
        az.storage.ManagementPolicy(
            f'{name}-management-policy',
            account_name=self.storage_account.name,
            resource_group_name=self.resource_group.name,
            policy=az.storage.ManagementPolicySchemaArgs(
                rules=lifecycle_rules
            )
        )

        return az.storage.BlobContainer(
            f'{name}',
            account_name=self.storage_account.name,
            resource_group_name=project or self.resource_group.name,
            container_name=name,
            metadata={'bucket': name},
            public_access=az.storage.PublicAccess.BLOB,
            # TODO: work out requester_pays in Azure
        )

    def add_member_to_bucket(
        self, resource_key: str, bucket, member, membership
    ) -> Any:
        az.authorization.RoleAssignment(
            resource_key,
            scope=bucket,
            principal_id=member,
            role_definition_id='Contributor',
            role_assignment_name='Storage Blob Data Contributor',
        )

    def create_machine_account(
        self, name: str, project: str = None, *, resource_key: str = None
    ) -> Any:
        application = az.batch.Application(
            resource_key or f'service-account-{name}',
            display_name=name,
            account_name=self._sanatize(name),
            resource_group_name=project or self.resource_group.name,
        )
        return application

    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member
    ) -> Any:
        pass

    def get_credentials_for_machine_account(self, resource_key, account):
        pass

    def create_group(self, name: str) -> Any:
        mail = f'{name}@populationgenomics.org.au'
        return az.management.ManagementGroup(
            name,
            display_name=name,
            group_id=mail,
        )

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

    def add_secret_version(
        self,
        resource_key: str,
        secret: Any,
        contents: Any,
        processor: Callable[[Any], Any] = None,
    ):
        pass

    def add_member_to_container_registry(
        self, resource_key: str, registry, member, membership, project=None
    ) -> Any:
        pass

    def give_member_ability_to_list_buckets(
        self, resource_key: str, member, project: str = None
    ):
        pass
