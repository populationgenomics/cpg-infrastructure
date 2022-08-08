"""
Generic Infrastructure abstraction that relies on each to be subclassed
by an equivalent GCP / Azure implementation.

Some challenges I forsee with this abstraction:

- Adding users to resources OUTSIDE this dataset,
    * BUCKETS:
        * GCP just need bucket_name
        * Azure need storage account + bucket name
    * Artifact registry:
        * GCP needs project + name
        * Azure needs storage account + registry name

"""

from abc import ABC, abstractmethod
from typing import Any

from cpg_infra.config import CPGDatasetConfig

UNDELETE_DAYS = 30


class CloudInfraBase(ABC):
    def __init__(self, config: CPGDatasetConfig):
        super().__init__()
        self.dataset = config.dataset

    @abstractmethod
    def rule_undelete(self, days=UNDELETE_DAYS) -> Any:
        """
        Return a lifecycle_rule that stores data for n days after delete"""
        pass

    # BUCKET

    @abstractmethod
    def create_bucket(self, name: str, lifecycle_rules: list, unique=False) -> Any:
        """
        This should take a potentially `non-unique` bucket name,
        and create a bucket, returning a resource.
        """
        pass

    @abstractmethod
    def add_member_to_bucket(self, resource_key: str, bucket, member) -> Any:
        """
        Add some member to a bucket.
        Note: You MUST specify a unique resource_key
        """
        pass

    # MACHINE ACCOUNTS
    @abstractmethod
    def create_machine_account(self, name: str) -> Any:
        """
        Generate a non-person account with some name
        """
        pass

    @abstractmethod
    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member
    ) -> Any:
        pass

    # GROUPS
    @abstractmethod
    def create_group(self, name: str) -> Any:
        """
        Create a GROUP, which is a proxy for a number of members
        """
        pass

    @abstractmethod
    def add_group_member(self, resource_key: str, group, member) -> Any:
        """
        Add some member to a GROUP
        Note: You MUST specify a unique resource_key
        """
        pass

    # SECRETS

    @abstractmethod
    def create_secret(self, name: str) -> Any:
        pass

    @abstractmethod
    def add_secret_member_accessor(self, resource_key: str, secret, member) -> Any:
        pass

    # ARTIFACT REPOSITORY

    @abstractmethod
    def add_member_to_artifact_registry(
        self, resource_key: str, artifact_registry, member
    ) -> Any:
        # TODO: this might need more thought
        pass


# DEV OVERRIDE


class DevInfra(CloudInfraBase):
    def rule_undelete(self, days=UNDELETE_DAYS) -> Any:
        return None

    def create_bucket(self, name: str, lifecycle_rules: list, unique=False) -> Any:
        print(f"Create bucket: {name}")
        return f"BUCKET://{name}"

    def add_member_to_bucket(self, resource_key: str, bucket, member):
        print(f"{resource_key} :: Add {member} to {bucket}")

    def create_machine_account(self, name: str) -> Any:
        print(f"Creating SA: {name}")
        return name + "@generated.service-account"

    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member
    ) -> Any:
        print(f"Allow {member} to access {machine_account}")

    def create_group(self, name: str) -> Any:
        print(f"Creating Group: {name}")
        return name + "@populationgenomics.org.au"

    def add_group_member(self, resource_key: str, group, member) -> Any:
        print(f"{resource_key} :: Add {member} to {group}")

    def create_secret(self, name: str) -> Any:
        print(f"Creating secret: {name}")
        return f"SECRET:{name}"

    def add_secret_member_accessor(self, resource_key: str, secret, member) -> Any:
        print(f"{resource_key} :: Allow {member} to read secret {secret}")

    def add_member_to_artifact_registry(
        self, resource_key: str, artifact_registry, member
    ) -> Any:
        pass
