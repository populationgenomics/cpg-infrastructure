# pylint: disable=missing-class-docstring, missing-function-docstring,too-many-public-methods
"""
Azure implementation for abstract infrastructure
"""
from datetime import date

from typing import Any
from functools import cached_property

from pulumi import Output
import pulumi_azure_native as az
import pulumi_azuread as azuread

from cpg_infra.config import CPGInfrastructureConfig, CPGDatasetConfig
from cpg_infra.abstraction.base import (
    CloudInfraBase,
    UNDELETE_PERIOD_IN_DAYS,
    ARCHIVE_PERIOD_IN_DAYS,
    TMP_BUCKET_PERIOD_IN_DAYS,
    SecretMembership,
    BucketMembership,
)

AZURE_BILLING_START_DATE = '2017-06-01T00:00:00Z'
AZURE_BILLING_EXPIRY_DATE = '3141-25-09T00:00:00Z'


class AzureInfra(CloudInfraBase):
    def __init__(
        self, config: CPGInfrastructureConfig, dataset_config: CPGDatasetConfig
    ):
        super().__init__(config, dataset_config)

        self.region = 'australiaeast'
        self.prefix = config.dataset_storage_prefix.replace('-', '')
        self._resource_group_name = f'{self.prefix}{self.dataset}'
        self._storage_account_name = f'{self.prefix}{self.dataset}'
        self.storage_account_lifecycle_rules = []
        self.storage_account_undelete_rule = None
        
        data = az.authorization.get_client_config()
        self.subscription = '/subscriptions/' + data.subscription_id
        self.tenant = data.tenant_id

    def finalise(self):
        """The azure storage account has a single management policy, and all the
        lifecycle rules need to be applied at once"""

        self._create_management_policy()
        if self.storage_account_undelete_rule:
            self._undelete(self.storage_account_undelete_rule)

    @staticmethod
    def name():
        return 'azure'

    def get_dataset_project_id(self):
        return self.dataset

    @cached_property
    def resource_group(self):
        return az.resources.ResourceGroup(
            self._resource_group_name,
            location=self.region,
        )

    @cached_property
    def storage_account(self):
        return az.storage.StorageAccount(
            self._storage_account_name,
            resource_group_name=self.resource_group.name,
            location=self.region,
            kind='StorageV2',
            sku=az.storage.SkuArgs(name='Standard_LRS'),
        )

    def _create_management_policy(self):
        return az.storage.ManagementPolicy(
            f'{self.storage_account.name}-management-policy',
            account_name=self.storage_account.name,
            resource_group_name=self.resource_group.name,
            policy=az.storage.ManagementPolicySchemaArgs(
                rules=self.storage_account_lifecycle_rules,
            ),
            management_policy_name='default',
        )

    def create_project(self, name):
        # TODO: Check if this will be final implementation of shared projects in az
        return az.resources.ResourceGroup(name)

    def create_budget(
        self,
        resource_key: str,
        *,
        project,
        budget: int,
        budget_filter: az.consumption.BudgetArgs,
    ):
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
            category='Cost',
            scope=self.subscription,
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
        filters = az.consumption.BudgetFilterArgs(
            and_=[
                az.consumption.BudgetFilterPropertiesArgs(
                    dimensions=az.consumption.BudgetComparisonExpressionArgs(
                        name='ResourceId',
                        operator='In',
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
                time_grain='Annually',
                time_period=az.consumption.BudgetTimePeriodArgs(
                    start_date=str(start_date), end_date=AZURE_BILLING_EXPIRY_DATE
                ),
                filter=filters,
            ),
        )

    def create_monthly_budget(self, resource_key: str, *, project, budget: int):
        # No start date here thats an issue
        filters = az.consumption.BudgetFilterArgs(
            and_=[
                az.consumption.BudgetFilterPropertiesArgs(
                    dimensions=az.consumption.BudgetComparisonExpressionArgs(
                        name='ResourceId',
                        operator='In',
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
                    end_date=AZURE_BILLING_EXPIRY_DATE,
                ),
                filter=filters,
            ),
        )

    def _undelete(self, days=UNDELETE_PERIOD_IN_DAYS):
        az.storage.BlobServiceProperties(
            f'{self.storage_account.name}-{days}day-undelete-rule',
            account_name=self.storage_account.name,
            blob_services_name='default',
            delete_retention_policy=az.storage.DeleteRetentionPolicyArgs(
                days=days, enabled=True
            ),
            resource_group_name=self.resource_group.name,
        )

    def bucket_rule_undelete(self, days=UNDELETE_PERIOD_IN_DAYS) -> Any:
        """
        These rules cannot be applied on a per bucket basis.
        Instead, a delete retention policy applies to all blobs within a
        Storage Account.
        This function sets the number of days for the delete retention policy.
        This service property gets applied and activated during
        finalise()
        """
        self.storage_account_undelete_rule = days

    def bucket_rule_archive(self, days=ARCHIVE_PERIOD_IN_DAYS) -> Any:
        # TODO: Remove filters here on account of it being applied consistently in create_bucket function
        return az.storage.ManagementPolicyRuleArgs(
            name='bucket-rule-archive',
            type='Lifecycle',
            definition=az.storage.ManagementPolicyDefinitionArgs(
                actions=az.storage.ManagementPolicyActionArgs(
                    base_blob=az.storage.ManagementPolicyBaseBlobArgs(
                        tier_to_archive=az.storage.DateAfterModificationArgs(
                            days_after_modification_greater_than=days,
                        )
                    )
                ),
                filters=az.storage.ManagementPolicyFilterArgs(
                    blob_types=['blockBlob'],
                    prefix_match=['olcmtestcontainer1'],
                ),
            ),
        )

    def bucket_rule_temporary(self, days=TMP_BUCKET_PERIOD_IN_DAYS) -> Any:
        return az.storage.ManagementPolicyRuleArgs(
            name='bucket-rule-tmp',
            type='Lifecycle',
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
            prefix_match=[name], blob_types=['blockBlob']
        )

        def apply_filter(rule):
            rule.definition.filters = bucket_filter
            rule.name = f'{name}-{rule.name}'
            return rule

        lifecycle_rules = filter(lambda x: x, lifecycle_rules)
        lifecycle_rules = list(map(apply_filter, lifecycle_rules))

        self.storage_account_lifecycle_rules.extend(lifecycle_rules)

        return az.storage.BlobContainer(
            f'{name}',
            account_name=self.storage_account.name,
            resource_group_name=project or self.resource_group.name,
            container_name=name,
            metadata={'bucket': name},
            # TODO: work out requester_pays in Azure
        )

    def bucket_membership_to_role(self, membership: BucketMembership):
        if membership == BucketMembership.MUTATE or membership == BucketMembership.APPEND:
            return f'{self.subscription}/providers/Microsoft.Authorization/roleDefinitions/ba92f5b4-2d11-453d-a403-e96b0029c9fe'
        if membership == BucketMembership.READ or BucketMembership.LIST:
            return f'{self.subscription}/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'

        raise ValueError(f'Unrecognised bucket membership type {membership}')

    def add_member_to_bucket(
        self, resource_key: str, bucket, member, membership
    ) -> Any:
        if hasattr(member, 'principal_id'):
            principal_type = 'ServicePrincipal'
        elif isinstance(member, azuread.group.Group):
            principal_type = 'Group' 
        else:
            principal_type = 'User'

        pid = member.principal_id if hasattr(member, 'principal_id') else member.id

        return az.authorization.RoleAssignment(
            resource_key,
            scope=bucket.id,
            principal_id=pid,
            principal_type=principal_type,
            role_definition_id=self.bucket_membership_to_role(membership),
        )

    def create_machine_account(
        self, name: str, project: str = None, *, resource_key: str = None
    ) -> Any:
        return az.managedidentity.UserAssignedIdentity(
            resource_key or f'service-account-{name}',
            location=self.region,
            resource_group_name=self.resource_group.name,
        )

    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member, project: str = None
    ) -> Any:
        pass

    def get_credentials_for_machine_account(self, resource_key, account):
        pass

    def create_group(self, name: str) -> Any:
        return azuread.Group(
            name,
            display_name=name,
            security_enabled=True,
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
