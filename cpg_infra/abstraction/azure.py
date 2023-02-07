# pylint: disable=missing-class-docstring, missing-function-docstring,too-many-public-methods
"""
Azure implementation for abstract infrastructure

If we want custom role / permissions, potentially look at:
    https://www.pulumi.com/registry/packages/azure-native/api-docs/authorization/roledefinition/
"""
import re
from datetime import date

from typing import Any
from functools import cached_property

import pulumi
import pulumi_azure_native as az
import pulumi_azuread as azuread

from cpg_infra.config import CPGInfrastructureConfig, CPGDatasetConfig
from cpg_infra.abstraction.base import (
    CloudInfraBase,
    SecretMembership,
    BucketMembership,
    UNDELETE_PERIOD_IN_DAYS,
    ARCHIVE_PERIOD_IN_DAYS,
    TMP_BUCKET_PERIOD_IN_DAYS,
    ContainerRegistryMembership,
)

AZURE_BILLING_START_DATE = '2017-06-01T00:00:00Z'
AZURE_BILLING_EXPIRY_DATE = '3141-25-09T00:00:00Z'


class AzureInfra(CloudInfraBase):
    def __init__(
        self, config: CPGInfrastructureConfig, dataset_config: CPGDatasetConfig
    ):
        super().__init__(config, dataset_config)

        assert config.azure, 'config.azure is required to deploy to Azure'

        self.region = config.azure.region
        self._resource_group_name = f'{config.dataset_storage_prefix}{self.dataset}'
        self._storage_account_name = self.fix_azure_alphanum_names(
            f'{config.dataset_storage_prefix}{self.dataset}'
        )
        self.storage_account_lifecycle_rules = []
        self.storage_account_undelete_rule = None

        data = az.authorization.get_client_config()
        self.subscription_id = '/subscriptions/' + data.subscription_id
        self.tenant = data.tenant_id

    def finalise(self):
        """The azure storage account has a single management policy, and all the
        lifecycle rules need to be applied at once"""
        # manually force a storage account to be created
        _ = self.storage_account
        self._create_management_policy()
        if self.storage_account_undelete_rule:
            self._undelete(self.storage_account_undelete_rule)

    @staticmethod
    def name():
        return 'azure'

    def get_dataset_project_id(self):
        return self.dataset

    @staticmethod
    def fix_azure_alphanum_names(name):
        return re.sub('[^a-z]', '', name.lower())

    @staticmethod
    def member_id(member):
        raise NotImplementedError

    @cached_property
    def resource_group(self):
        return az.resources.ResourceGroup(
            self.get_pulumi_name(self._resource_group_name),
            resource_group_name=self._resource_group_name,
            location=self.region,
        )

    @cached_property
    def storage_account(self):
        return az.storage.StorageAccount(
            self.get_pulumi_name(self._storage_account_name),
            account_name=self._storage_account_name,
            resource_group_name=self.resource_group.name,
            location=self.region,
            kind='StorageV2',
            sku=az.storage.SkuArgs(name='Standard_LRS'),
        )

    def bucket_output_path(self, bucket):
        return pulumi.Output.concat(
            'hail-az://', self.storage_account.name, '/', bucket.name
        )

    def _create_management_policy(self):
        if not self.storage_account_lifecycle_rules:
            return None

        return az.storage.ManagementPolicy(
            self.get_pulumi_name(f'{self._storage_account_name}-management-policy'),
            account_name=self.storage_account.name,
            resource_group_name=self.resource_group.name,
            policy=az.storage.ManagementPolicySchemaArgs(
                rules=self.storage_account_lifecycle_rules,
            ),
            management_policy_name='default',
        )

    def create_project(self, name):
        return az.resources.ResourceGroup(name)

    def create_budget(
        self,
        resource_key: str,
        *,
        project,
        budget: int,
        budget_filter: az.consumption.BudgetArgs,
    ):
        raise NotImplementedError
        # kwargs = {}
        # # if self.config.gcp.budget_notification_pubsub:
        # #     kwargs['threshold_rules'] = [
        # #         gcp.billing.BudgetThresholdRuleArgs(threshold_percent=threshold)
        # #         for threshold in self.config.budget_notification_thresholds
        # #     ]
        # #     kwargs['all_updates_rule'] = gcp.billing.BudgetAllUpdatesRuleArgs(
        # #         pubsub_topic=self.config.gcp.budget_notification_pubsub,
        # #         schema_version='1.0',
        # #     )
        #
        # filters = budget_filter.pop('filter')
        # kwargs = dict(kwargs, dict(budget_filter))
        #
        # az.consumption.Budget(
        #     self.get_pulumi_name(resource_key),
        #     budget_name=f'{project.name}-budget',
        #     amount=budget,
        #     category='Cost',
        #     scope=self.subscription,
        #     budget_filter=filters,
        #     **kwargs,
        # )

    def create_fixed_budget(
        self,
        resource_key: str,
        *,
        project,
        budget: int,
        start_date: date = date(2022, 1, 1),
    ):
        raise NotImplementedError
        # filters = az.consumption.BudgetFilterArgs(
        #     and_=[
        #         az.consumption.BudgetFilterPropertiesArgs(
        #             dimensions=az.consumption.BudgetComparisonExpressionArgs(
        #                 name='ResourceId',
        #                 operator='In',
        #                 values=[project.id],
        #             ),
        #         )
        #     ]
        # )
        # return self.create_budget(
        #     resource_key=self.get_pulumi_name(resource_key),
        #     project=project,
        #     budget=budget,
        #     budget_filter=az.consumption.BudgetArgs(
        #         time_grain='Annually',
        #         time_period=az.consumption.BudgetTimePeriodArgs(
        #             start_date=str(start_date), end_date=AZURE_BILLING_EXPIRY_DATE
        #         ),
        #         filter=filters,
        #     ),
        # )

    def create_monthly_budget(self, resource_key: str, *, project, budget: int):
        raise NotImplementedError
        # # No start date here that's an issue
        # filters = az.consumption.BudgetFilterArgs(
        #     and_=[
        #         az.consumption.BudgetFilterPropertiesArgs(
        #             dimensions=az.consumption.BudgetComparisonExpressionArgs(
        #                 name='ResourceId',
        #                 operator='In',
        #                 values=[project.id],
        #             ),
        #         )
        #     ]
        # )
        # return self.create_budget(
        #     resource_key=self.get_pulumi_name(resource_key),
        #     project=project,
        #     budget=budget,
        #     budget_filter=az.consumption.BudgetArgs(
        #         time_grain='Monthly',
        #         time_period=az.consumption.BudgetTimePeriodArgs(
        #             start_date=AZURE_BILLING_START_DATE,
        #             end_date=AZURE_BILLING_EXPIRY_DATE,
        #         ),
        #         filter=filters,
        #     ),
        # )

    def _undelete(self, days=UNDELETE_PERIOD_IN_DAYS):
        az.storage.BlobServiceProperties(
            self.get_pulumi_name(
                f'{self._storage_account_name}-{days}day-undelete-rule'
            ),
            account_name=self.storage_account.name,
            blob_services_name='default',
            delete_retention_policy=az.storage.DeleteRetentionPolicyArgs(
                days=days, enabled=True
            ),
            resource_group_name=self.resource_group.name,
        )

    def bucket_rule_undelete(self, days=UNDELETE_PERIOD_IN_DAYS) -> Any:
        """
        These rules cannot be applied on a per-bucket basis.
        Instead, a delete-retention-policy applies to all blobs within a
        Storage Account.
        This function sets the number of days for the delete-retention-policy.
        This service property gets applied and activated during
        finalise()
        """
        self.storage_account_undelete_rule = days

    def bucket_rule_archive(self, days=ARCHIVE_PERIOD_IN_DAYS) -> Any:
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
        autoclass: bool = False,
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

        resource = az.storage.BlobContainer(
            self.get_pulumi_name(name + '-blob-container'),
            account_name=self.storage_account.name,
            resource_group_name=project or self.resource_group.name,
            container_name=name,
            metadata={'bucket': name},
            # TODO: work out requester_pays in Azure
        )

        # Autoclass is not supported on Azure. We don't assert here, as some datasets
        # exist on both GCP (where Autoclass is enabled) and Azure, like cpg-common.
        if autoclass:
            pulumi.warn('Ignoring `autoclass` on Azure', resource=resource)

        # Requester Pays is not available on Azure.
        if requester_pays:
            pulumi.warn('Ignoring `requester_pays` on Azure', resource=resource)

        return resource

    @staticmethod
    def bucket_membership_to_role(membership: BucketMembership):
        """
        WARNING: there is no LIST, only read + list / write
        Get the role for a specific BucketMembership.
        """
        # role_blob_owners = '/providers/Microsoft.Authorization/roleDefinitions/b7e6dc6d-f1e8-4753-8033-0f276bb0955b'
        role_blob_reader = '/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'
        role_blob_contributor = '/providers/Microsoft.Authorization/roleDefinitions/ba92f5b4-2d11-453d-a403-e96b0029c9fe'

        if membership in (BucketMembership.MUTATE, BucketMembership.APPEND):
            return role_blob_contributor
        if membership in (BucketMembership.READ, BucketMembership.LIST):
            return role_blob_reader

        raise ValueError(f'Unrecognised bucket membership type {membership}')

    def add_member_to_bucket(
        self, resource_key: str, bucket, member, membership
    ) -> Any:
        return az.authorization.RoleAssignment(
            self.get_pulumi_name(resource_key),
            scope=self._get_object_id(bucket),
            principal_id=self._get_object_id(member),
            principal_type=self._get_principal_type(member),
            role_definition_id=self.bucket_membership_to_role(membership),
        )

    @staticmethod
    def _get_principal_type(obj):
        # it's a 'cpg_infra.driver.CPGInfrastructure.GroupProvider.Group'
        if hasattr(obj, 'is_group') and hasattr(obj, 'group'):
            # cheeky catch for internal group
            return AzureInfra._get_principal_type(obj.group)

        if isinstance(obj, az.managedidentity.UserAssignedIdentity):
            return 'ServicePrincipal'
        if isinstance(obj, azuread.group.Group):
            return 'Group'
        if isinstance(obj, str):
            # we don't have cases yet where we want to add a user by string, so sort of kludge
            return 'ServicePrincipal'

        raise ValueError(f'Unrecognised principal {obj} (type: {type(obj)})')

    def add_blob_to_bucket(self, resource_name, bucket, output_name, contents):
        raise NotImplementedError

    def create_machine_account(
        self, name: str, project: str = None, *, resource_key: str = None
    ) -> Any:
        return az.managedidentity.UserAssignedIdentity(
            self.get_pulumi_name((resource_key or f'service-account-{name}')),
            resource_name_=name,
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
            self.get_pulumi_name(name + '-group'),
            display_name=name,
            security_enabled=True,
        )

    @staticmethod
    def _get_object_id(obj):  # pylint: disable=too-many-return-statements
        if hasattr(obj, 'is_group') and hasattr(obj, 'group'):
            # cheeky catch for internal group
            return AzureInfra._get_object_id(obj.group)

        if isinstance(obj, pulumi.Output):
            return obj

        if isinstance(obj, str):
            return obj

        if isinstance(obj, az.managedidentity.UserAssignedIdentity):
            return obj.principal_id

        if isinstance(obj, azuread.Group):
            return obj.id

        if isinstance(obj, az.containerregistry.Registry):
            return obj.id

        if isinstance(obj, az.storage.BlobContainer):
            return obj.id

        raise ValueError(f'Unrecognised object: {obj} ({type(obj)})')

    def add_group_member(
        self, resource_key: str, group, member, unique_resource_key: bool = False
    ) -> Any:
        if not unique_resource_key:
            resource_key = self.get_pulumi_name(resource_key)
        return azuread.GroupMember(
            resource_key,
            group_object_id=self._get_object_id(group),
            member_object_id=self._get_object_id(member),
        )

    @cached_property
    def secret_vault(self):
        return az.keyvault.Vault(
            self.get_pulumi_name('vault'),
            resource_group_name=self.resource_group.name,
            vault_name='secrets',
            location=self.region,
            properties=az.keyvault.VaultPropertiesArgs(
                tenant_id=self.tenant,
                enabled_for_deployment=True,
                enabled_for_disk_encryption=True,
                enabled_for_template_deployment=True,
            ),
        )

    def create_secret(self, name: str, project: str = None) -> Any:
        return az.keyvault.Secret(
            self.get_pulumi_name('secret-' + name),
            secret_name=name,
            properties=az.keyvault.SecretPropertiesArgs(
                value=None,
            ),
            resource_group_name=self.resource_group.name,
            vault_name=self.secret_vault.name,
        )

    def add_secret_version(
        self,
        resource_key: str,
        secret: Any,
        contents: Any,
    ):
        return az.keyvault.Secret(
            self.get_pulumi_name(resource_key),
            secret_name=secret.name,
            properties=az.keyvault.SecretPropertiesArgs(
                value=contents,
            ),
            resource_group_name=self.resource_group.name,
            vault_name=self.secret_vault.name,
        )

    def add_secret_member(
        self,
        resource_key: str,
        secret,
        member,
        membership: SecretMembership,
        project: str = None,
    ) -> Any:
        pass

    @staticmethod
    def _container_membership_to_roles(
        membership: ContainerRegistryMembership,
    ) -> dict[str, str]:
        role_pull = '/providers/Microsoft.Authorization/roleDefinitions/7f951dda-4ed3-4680-a7ca-43fe172d538d'
        role_push = '/providers/Microsoft.Authorization/roleDefinitions/8311e382-0749-4cb8-b61a-304f252e45ec'
        role_delete = '/providers/Microsoft.Authorization/roleDefinitions/c2f4ef07-c644-48eb-af81-4b1b4947fb11'
        if membership == ContainerRegistryMembership.READER:
            return {'pull': role_pull}
        if membership == ContainerRegistryMembership.WRITER:
            return {'pull': role_pull, 'push': role_push, 'delete': role_delete}

        raise ValueError(f'Unknown container membership: {membership}')

    def add_member_to_container_registry(
        self, resource_key: str, registry, member, membership, project=None
    ) -> list[Any]:
        roles = []
        for role_type, role in self._container_membership_to_roles(membership).items():
            roles.append(
                az.authorization.RoleAssignment(
                    self.get_pulumi_name(resource_key + '-' + role_type),
                    principal_id=self._get_object_id(member),
                    principal_type=self._get_principal_type(member),
                    role_assignment_name=None,
                    role_definition_id=role,
                    scope=self._get_object_id(registry),
                )
            )

        return roles

    def give_member_ability_to_list_buckets(
        self, resource_key: str, member, project: str = None
    ):
        return az.authorization.RoleAssignment(
            self.get_pulumi_name(resource_key),
            scope=self.storage_account.id,
            principal_id=self._get_object_id(member),
            principal_type=self._get_principal_type(member),
            role_definition_id=self.bucket_membership_to_role(BucketMembership.LIST),
        )

    def create_container_registry(self, name: str):
        return az.containerregistry.Registry(
            self.get_pulumi_name(f'container-registry-{name}'),
            admin_user_enabled=True,
            location=self.region,
            registry_name=self.fix_azure_alphanum_names(
                self.config.dataset_storage_prefix + self.dataset + name
            ),
            resource_group_name=self.resource_group.name,
            sku=az.containerregistry.SkuArgs(
                name='Basic',
            ),
        )
