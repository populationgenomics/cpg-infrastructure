# pylint: disable=missing-function-docstring,unnecessary-pass
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
from datetime import date
from enum import Enum
from typing import Any, Callable

from cpg_infra.config import (
    CPGDatasetConfig,
    CPGDatasetComponents,
    CPGInfrastructureConfig,
)

UNDELETE_PERIOD_IN_DAYS = 30
TMP_BUCKET_PERIOD_IN_DAYS = 8  # tmp content gets deleted afterwards.
ARCHIVE_PERIOD_IN_DAYS = 30


class SecretMembership(Enum):
    """Secret membership pattern"""

    ACCESSOR = 'accessor'
    ADMIN = 'admin'


class BucketMembership(Enum):
    """Membership type for a bucket"""

    LIST = 'list'
    READ = 'read'
    APPEND = 'append'
    MUTATE = 'mutate'


class ContainerRegistryMembership(Enum):
    """Container registry membership type"""

    READER = 'reader'
    APPEND = 'append'


class CloudInfraBase(ABC):
    """
    Base class for interacting with a specific cloud. ALL methods
    should be implemented to ensure the driver works correctly.

    This class was designed to work with Pulumi, ie: all these methods
    ensure resources are created (and take no action if they're already created).
    Resources should be able to determine the unique key, but all memberships
    require the driver to specify a unique key to link memberships.
    """

    def __init__(
        self, config: CPGInfrastructureConfig, dataset_config: CPGDatasetConfig
    ):
        super().__init__()
        self.config = config
        self.dataset = dataset_config.dataset
        self.components = dataset_config.components.get(
            self.name(),
            CPGDatasetComponents.default_component_for_infrastructure()[self.name()],
        )

    @staticmethod
    @abstractmethod
    def name():
        pass

    # region PROJECT
    @abstractmethod
    def get_dataset_project_id(self):
        pass

    @abstractmethod
    def create_project(self, name):
        pass

    @abstractmethod
    def create_monthly_budget(self, resource_key: str, *, project, budget):
        pass

    @abstractmethod
    def create_fixed_budget(
        self, resource_key: str, *, project, budget, start_date: date = date(2022, 1, 1)
    ):
        pass

    # region PROJECT

    # region BUCKET
    @abstractmethod
    def bucket_rule_undelete(self, days=UNDELETE_PERIOD_IN_DAYS) -> Any:
        """
        Return a lifecycle_rule that stores data for n days after delete"""
        pass

    @abstractmethod
    def bucket_rule_temporary(self, days=TMP_BUCKET_PERIOD_IN_DAYS) -> Any:
        """
        Return a lifecycle_rule that stores data for n days after delete"""
        pass

    @abstractmethod
    def bucket_rule_archive(self, days=ARCHIVE_PERIOD_IN_DAYS) -> Any:
        pass

    @abstractmethod
    def create_bucket(
        self,
        name: str,
        lifecycle_rules: list,
        unique: bool = False,
        requester_pays: bool = False,
        versioning: bool = True,
        project: str = None,
    ) -> Any:
        """
        This should take a potentially `non-unique` bucket name,
        and create a bucket, returning a resource.
        :param requester_pays:
        """
        pass

    @abstractmethod
    def add_member_to_bucket(
        self, resource_key: str, bucket, member, membership: BucketMembership
    ) -> Any:
        """
        Add some member to a bucket.
        Note: You MUST specify a unique resource_key
        :param membership:
        """
        pass

    @abstractmethod
    def give_member_ability_to_list_buckets(
        self, resource_key: str, member, project: str = None
    ):
        pass

    # endregion BUCKET

    # region MACHINE ACCOUNTS
    @abstractmethod
    def create_machine_account(
        self, name: str, project: str = None, *, resource_key: str = None
    ) -> Any:
        """
        Generate a non-person account with some name
        :param project:
        """
        pass

    @abstractmethod
    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member
    ) -> Any:
        pass

    @abstractmethod
    def get_credentials_for_machine_account(self, resource_key, account):
        pass

    # endregion MACHINE ACCOUNTS
    # GROUPS
    @abstractmethod
    def create_group(self, name: str) -> Any:
        """
        Create a GROUP, which is a proxy for a number of members
        """

    @abstractmethod
    def add_group_member(self, resource_key: str, group, member) -> Any:
        pass

    # SECRETS

    @abstractmethod
    def create_secret(self, name: str, project: str = None) -> Any:
        pass

    @abstractmethod
    def add_secret_member(
        self,
        resource_key: str,
        secret,
        member,
        membership: SecretMembership,
        project: str = None,
    ) -> Any:
        pass

    @abstractmethod
    def add_secret_version(
        self,
        resource_key: str,
        secret: Any,
        contents: Any,
    ):
        pass

    # ARTIFACT REPOSITORY

    @abstractmethod
    def add_member_to_container_registry(
        self,
        resource_key: str,
        registry,
        member,
        membership: ContainerRegistryMembership,
        project: str = None,
    ) -> Any:
        # TODO: this might need more thought
        pass


# DEV OVERRIDE


class DevInfra(CloudInfraBase):
    """Dev infrastructure (just prints resources)"""

    @staticmethod
    def name():
        return 'dev'

    def get_dataset_project_id(self):
        return self.dataset

    def create_project(self, name):
        print(f'Creating project: {name}')
        return f'Project: {name}'

    def create_monthly_budget(self, resource_key: str, *, project, budget):
        print(
            f'{resource_key} :: Create monthly budget for {project}: ${budget} {self.config.budget_currency}'
        )

    def create_fixed_budget(
        self, resource_key: str, *, project, budget, start_date: date = date(2022, 1, 1)
    ):
        print(
            f'{resource_key} :: Create fixed budget for {project}: ${budget} {self.config.budget_currency} (from {date})'
        )

    def bucket_rule_undelete(self, days=UNDELETE_PERIOD_IN_DAYS) -> Any:
        return f'RULE:undelete={days}d'

    def bucket_rule_temporary(self, days=TMP_BUCKET_PERIOD_IN_DAYS) -> Any:
        return f'RULE:tmp={days}d'

    def bucket_rule_archive(self, days=ARCHIVE_PERIOD_IN_DAYS) -> Any:
        return f'RULE:archive={days}d'

    def create_bucket(
        self,
        name: str,
        lifecycle_rules: list,
        unique: bool = False,
        requester_pays: bool = False,
        versioning: bool = True,
        project: str = None,
    ) -> Any:
        print(f'Create bucket: {name} w/ rules: {", ".join(lifecycle_rules)}')
        return f'BUCKET://{name}'

    def add_member_to_bucket(self, resource_key: str, bucket, member, membership):
        print(f'{resource_key} :: Add {member} to {bucket}')

    def create_machine_account(
        self, name: str, project: str = None, *, resource_key: str = None
    ) -> Any:
        print(f'Creating SA: {name}')
        return name + '@generated.service-account'

    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member
    ) -> Any:
        print(f'Allow {member} to access {machine_account}')

    def get_credentials_for_machine_account(self, resource_key, account):
        return f'{resource_key} :: {account}.CREDENTIALS'

    def create_group(self, name: str) -> Any:
        print(f'Creating Group: {name}')
        return name + '@populationgenomics.org.au'

    def add_group_member(self, resource_key: str, group, member) -> Any:
        print(f'{resource_key} :: Add {member} to {group}')

    def create_secret(self, name: str, project: str = None) -> Any:
        print(f'Creating secret: {name}')
        return f'SECRET:{name}'

    def add_secret_member(
        self, resource_key: str, secret, member, membership, project: str = None
    ) -> Any:
        print(f'{resource_key} :: Allow {member} to read secret {secret}')

    def add_secret_version(
        self,
        resource_key: str,
        secret: Any,
        contents: Any,
        processor: Callable[[Any], Any] = None,
    ):
        _processor = processor or (lambda el: el)
        return f'{resource_key} :: {secret}.add_version("{_processor(contents)}")'

    def add_member_to_container_registry(
        self, resource_key: str, registry, member, membership, project=None
    ) -> Any:
        return f'{resource_key} :: Add {member} to CONTAINER registry {registry}'

    def give_member_ability_to_list_buckets(
        self, resource_key: str, member, project: str = None
    ):
        return f'{resource_key} :: {member} can list buckets'
