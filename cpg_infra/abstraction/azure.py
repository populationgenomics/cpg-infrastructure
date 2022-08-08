from typing import Any

import pulumi_azure_native as az

from cpg_infra.abstraction.base import CloudInfraBase, UNDELETE_DAYS
from cpg_infra.config import CPGDatasetConfig


class AzureInfra(CloudInfraBase):
    def __init__(self, config: CPGDatasetConfig):
        super().__init__(config)

        self.resource_group_name = f"cpg-{self.dataset}"
        self.storage_account_name = f"cpg-{self.dataset}"

        self._storage_account = None
        self._resource_group = None

    @property
    def resource_group(self):
        if not self._resource_group:
            self._resource_group = az.resources.ResourceGroup("resource_group")

        return self._resource_group

    @property
    def storage_account(self):
        if not self._storage_account:
            self._storage_account = az.storage.Account(
                "cpg-" + self.dataset, resource_group=self.resource_group
            )
        return self._storage_account

    def rule_undelete(self, days=UNDELETE_DAYS) -> Any:
        pass

    def create_bucket(self, name: str, lifecycle_rules: list, unique=False) -> Any:
        return az.storage.BlobContainer(
            f"bucket-{name}",
            resource_group_name=self.resource_group_name,
            account_name=self.storage_account_name,
            container_name=name,
        )

    def add_member_to_bucket(self, resource_key: str, bucket, member) -> Any:
        az.authorization.RoleAssignment(
            resource_key,
            scope=bucket.id,
            principal_id=member.id,
            role_definition_id='Contributor',
            role_assignment_name='Storage Blob Data Contributor',
        )

    def create_machine_account(self, name: str) -> Any:
        application = az.batch.Application(f'application-{name}', account_name=name, display_name=name, resource_group_name=self.resource_group_name)
        return application

    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member
    ) -> Any:
        pass

    def create_group(self, name: str) -> Any:
        pass

    def add_group_member(self, resource_key: str, group, member) -> Any:
        pass

    def create_secret(self, name: str) -> Any:
        pass

    def add_secret_member_accessor(self, resource_key: str, secret, member) -> Any:
        pass

    def add_member_to_artifact_registry(
        self, resource_key: str, artifact_registry, member
    ) -> Any:
        pass
