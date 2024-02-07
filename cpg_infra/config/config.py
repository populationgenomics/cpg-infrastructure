# flake8: noqa: ANN102,ANN204,ANN206,,C901,ANN401,PLR2004mERA001
"""
This module contains all the configuration objects that are used to
describe the CPG infrastructure, including what's required from a
specific dataset.
"""
import dataclasses
from enum import Enum
from typing import Any, Literal

import toml

from cpg_infra.config.deserializabledataclass import DeserializableDataclass

MemberKey = str
GroupType = str
CloudName = Literal['gcp', 'azure']
GroupName = Literal[
    'data-manager',
    'analysis',
    'metadata-access',
    'web-access',
    'release-access',
    'upload',
]


@dataclasses.dataclass(frozen=True)
class CPGInfrastructureUser(DeserializableDataclass):
    @dataclasses.dataclass(frozen=True)
    class Cloud(DeserializableDataclass):
        id: str  # noqa: A003
        hail_batch_username: str | None = None

    id: MemberKey  # noqa: A003
    clouds: dict[CloudName, Cloud]
    projects: list[str]
    add_to_internal_hail_batch_projects: bool = False


@dataclasses.dataclass(frozen=True)
class CPGInfrastructureConfig(DeserializableDataclass):
    """
    Configuration that describes all variables required to instantiate the
    CPG infrastructure.

    If we serialize the pulumi configurations + any other TOMLs, we can tell quickly
    if the configuration is correct and complete and make it simpler for tasks to use
    the correct keys.
    """

    @dataclasses.dataclass(frozen=True)
    class GCP(DeserializableDataclass):
        customer_id: str
        region: str
        groups_domain: str
        budget_notification_pubsub: str | None
        config_bucket_name: str
        dataset_storage_prefix: str

    @dataclasses.dataclass(frozen=True)
    class Azure(DeserializableDataclass):
        region: str
        subscription: str
        tenant: str
        dataset_storage_prefix: str
        config_bucket_name: str

    @dataclasses.dataclass(frozen=True)
    class Hail(DeserializableDataclass):
        @dataclasses.dataclass(frozen=True)
        class GCP(DeserializableDataclass):
            wheel_bucket_name: str
            hail_batch_url: str
            git_credentials_secret_name: str
            git_credentials_secret_project: str

        @dataclasses.dataclass(frozen=True)
        class Azure(DeserializableDataclass):
            hail_batch_url: str

        gcp: GCP
        azure: Azure | None = None

    @dataclasses.dataclass(frozen=True)
    class AnalysisRunner(DeserializableDataclass):
        @dataclasses.dataclass(frozen=True)
        class GCP(DeserializableDataclass):
            project: str
            cloud_run_instance_name: str
            server_machine_account: str
            logger_machine_account: str
            container_registry_name: str

        gcp: GCP

    @dataclasses.dataclass(frozen=True)
    class WebService(DeserializableDataclass):
        """
        This is a CPG-specific configuration that allows a
        web-server to serve static files from a bucket.
        """

        @dataclasses.dataclass(frozen=True)
        class GCP(DeserializableDataclass):
            server_machine_account: str

        gcp: GCP
        # The template is a string that can be formatted with: namespace, dataset
        web_url_template: str | None = None

    @dataclasses.dataclass(frozen=True)
    class Notebooks(DeserializableDataclass):
        @dataclasses.dataclass(frozen=True)
        class GCP(DeserializableDataclass):
            project: str

        gcp: GCP

    @dataclasses.dataclass(frozen=True)
    class Cromwell(DeserializableDataclass):
        @dataclasses.dataclass(frozen=True)
        class GCP(DeserializableDataclass):
            access_group_id: str
            runner_machine_account: str

        gcp: GCP

    @dataclasses.dataclass(frozen=True)
    class Metamist(DeserializableDataclass):
        @dataclasses.dataclass(frozen=True)
        class GCP(DeserializableDataclass):
            project: str
            service_name: str
            machine_account: str

        @dataclasses.dataclass(frozen=True)
        class ETLConfiguration(DeserializableDataclass):
            @dataclasses.dataclass(frozen=True)
            class ETLAccessorConfiguration(DeserializableDataclass):
                @dataclasses.dataclass(frozen=True)
                class ETLParserConfiguration(DeserializableDataclass):
                    # the type/version of the parser
                    name: str
                    # effectively, turn the
                    type_override: str | None
                    # Default ETL parser configuration, if not specified in ETL payload
                    # e.g.: {'project': 'greek-myth', 'default_sequencing_type': 'genome'}
                    default_parameters: dict[str, Any] | None = None

                parsers: list[ETLParserConfiguration]

            # Metamist environment (DEVELOPMENT / PRODUCTION) for ETL cloud functions
            accessors: dict[str, ETLAccessorConfiguration] | None
            environment: str | None = 'PRODUCTION'
            # Collection of private packages to be appended to requirements.txt
            private_repo_packages: list[str] | None = None

        gcp: GCP
        etl: ETLConfiguration | None = None
        slack_channel: str | None = None

    @dataclasses.dataclass(frozen=True)
    class Billing(DeserializableDataclass):
        @dataclasses.dataclass(frozen=True)
        class GCP(DeserializableDataclass):
            """Details of the BILLING account"""

            project_id: str
            account_id: str

        @dataclasses.dataclass(frozen=True)
        class GCPAggregator(DeserializableDataclass):
            source_bq_table: str
            destination_bq_table: str
            slack_channel: str
            slack_token_secret_name: str  # created in gcp.billing_project_id
            functions: list[str]
            billing_sheet_id: str | None = None
            monthly_summary_table: str | None = None
            interval_hours: int = 4

        coordinator_machine_account: str
        gcp: GCP
        aggregator: GCPAggregator | None = None
        hail_aggregator_username: str | None = None

    # used in the gcp.organizations.get_organization(domain=self.config.domain) call
    domain: str
    # Used when constructing budgets, usually AUD, USD, etc
    budget_currency: str
    # Which dataset should we use to place organisation-wide resources
    common_dataset: str

    # a gs://<bucket> path to a bucket to write storage, infra config files to
    config_destination: str

    # a map of users know to the system, noting that a CPGDatasetConfig lists the users
    # within itself, but this is a map of all users known to the system
    users: dict[MemberKey, CPGInfrastructureUser]

    # configuration options for GCP
    gcp: GCP | None = None
    # configuration options for Azure
    azure: Azure | None = None

    # configuration options for Hail Batch
    hail: Hail | None = None
    # configuration options for the analysis runner, the guard to analysis at the CPG
    analysis_runner: AnalysisRunner | None = None
    # configuration options for the web service, a server that serves static files
    # from a bucket
    web_service: WebService | None = None
    # configuration options for our notebooks service
    notebooks: Notebooks | None = None
    # configuration options for our cromwell service
    cromwell: Cromwell | None = None
    # configuration options for our metamist service
    metamist: Metamist | None = None
    # configuration options for billing + billing aggregation
    billing: Billing | None = None

    # When resources are renamed, it can be useful to explicitly apply changes in two
    # phases: delete followed by create; that's opposite of the default create followed by
    # delete, which can end up with missing permissions. To implement the first phase
    # (delete), simply change this to 'True', then revert to reapply group memberships
    disable_group_memberships: bool = False
    # sometimes it's useful to prefix the group names if you're using two different stacks
    # under the same organization. This allows you to avoid clashes :)
    group_prefix: str | None = None

    # The default budget notification thresholds
    budget_notification_thresholds: list[float] = dataclasses.field(
        default_factory=lambda: [0.5, 0.9, 1.0],
    )

    @staticmethod
    def from_toml(path: str) -> 'CPGInfrastructureConfig':
        with open(path, encoding='utf-8') as f:
            d = toml.load(f)
        return CPGInfrastructureConfig.from_dict(d)

    @staticmethod
    def from_dict(d: dict[str, Any]) -> 'CPGInfrastructureConfig':
        if 'infrastructure' in d:
            d = d['infrastructure']
        return CPGInfrastructureConfig(**d)


class CPGDatasetComponents(Enum):
    """
    The specific components that make up the dataset infrastructure
    """

    STORAGE = 'storage'
    SPARK = 'spark'
    CROMWELL = 'cromwell'
    NOTEBOOKS = 'notebooks'
    HAIL_ACCOUNTS = 'hail-accounts'
    SAMPLE_METADATA = 'sample_metadata'
    CONTAINER_REGISTRY = 'container-registry'
    ANALYSIS_RUNNER = 'analysis-runner'

    @staticmethod
    def default_component_for_infrastructure() -> (
        dict[str, list['CPGDatasetComponents']]
    ):
        return {
            'dry-run': list(CPGDatasetComponents),
            'gcp': list(CPGDatasetComponents),
            'azure': [
                CPGDatasetComponents.STORAGE,
                CPGDatasetComponents.HAIL_ACCOUNTS,
                CPGDatasetComponents.ANALYSIS_RUNNER,
                CPGDatasetComponents.CONTAINER_REGISTRY,
                # CPGDatasetComponents.SAMPLE_METADATA,
            ],
        }


@dataclasses.dataclass(frozen=True)
class HailAccount(DeserializableDataclass):
    """Represents a hail account on a specific cloud"""

    username: str
    cloud_id: str


@dataclasses.dataclass(frozen=True)
class CPGDatasetConfig(DeserializableDataclass):
    """
    Configuration that describes the minimum information
    required to construct the dataset infrastructure
    """

    def __post_init__(self):
        try:
            super().__post_init__()
        except TypeError as e:
            raise TypeError(
                f'Could not instantiate {self.__class__.__name__} for {self.dataset!r}: {e!s}',
            ) from e

    @dataclasses.dataclass(frozen=True)
    class Gcp(DeserializableDataclass):
        project: str
        region: str | None = None

        hail_service_account_test: HailAccount | None = None
        hail_service_account_standard: HailAccount | None = None
        hail_service_account_full: HailAccount | None = None

    @dataclasses.dataclass(frozen=True)
    class Azure(DeserializableDataclass):
        region: str | None = None

        hail_service_account_test: HailAccount | None = None
        hail_service_account_standard: HailAccount | None = None
        hail_service_account_full: HailAccount | None = None

    @dataclasses.dataclass(frozen=True)
    class Budget(DeserializableDataclass):
        # dollars

        monthly_budget: int
        shared_total_budget: int | None = None
        # if overriding from the default CpgInfrastructure.currency
        currency: str | None = None

    # the name of the dataset
    dataset: str

    # the budgets of the dataset, keyed by the cloud ID
    budgets: dict[CloudName, Budget]

    # GCP config options, noting GCP is a required target, so you must provide this
    gcp: Gcp
    # Azure config options
    azure: Azure | None = None

    # should we setup the test namespace (buckets, accounts, etc)
    # useful if you don't want to allow debugging for a dataset
    setup_test: bool = True

    # 2024-01-05 mfranklin: these deployment accounts are legacy, and could probably
    #   be removed, they relate to seqr's access to data, but we generally push.
    deployment_service_account_test: str | None = None
    deployment_service_account_standard: str | None = None
    deployment_service_account_full: str | None = None

    # create a container registry in the dataset's project, recommended for 'common'
    create_container_registry: bool = False

    # which clouds do you want to deploy to?
    deploy_locations: list[CloudName] = dataclasses.field(
        default_factory=lambda: ['gcp'],
    )

    is_internal_dataset: bool = False

    # creates a release requester-pays bucket
    enable_release: bool = False
    # creates a shared project + SA to manage egress costs from release bucket
    enable_shared_project: bool = False
    # creates a metamist project (+ test metamist project if setup_test is True)
    enable_metamist_project: bool = True

    # give FULL access to these datasets, as this dataset depends_on them
    depends_on: list[str] = dataclasses.field(default_factory=list)
    # give READONLY access to these datasets, as this dataset needs it
    depends_on_readonly: list[str] = dataclasses.field(default_factory=list)

    # extra places that collaborators can upload data too
    additional_upload_buckets: list[str] = dataclasses.field(default_factory=list)

    # convenience place for plumbing extra service-accounts for SM
    sm_read_only_sas: list[str] = dataclasses.field(default_factory=list)
    sm_read_write_sas: list[str] = dataclasses.field(default_factory=list)

    # Grace period for archive storage tier buckets.
    archive_age: int = 0

    # Whether to use Autoclass (https://cloud.google.com/storage/docs/autoclass)
    # for non-archive buckets. Currently only supported on GCP.
    autoclass: bool = True

    # which components should this dataset deploy on each cloud
    components: dict[CloudName, list[CPGDatasetComponents]] = dataclasses.field(
        default_factory=dict,
    )

    # Which users to do you want to be a part of each group.
    members: dict[GroupName, list[MemberKey]] = dataclasses.field(default_factory=dict)

    @classmethod
    def instantiate(cls, **kwargs: dict[str, Any]):
        if components := kwargs.get('components'):
            kwargs['components'] = {
                k: [CPGDatasetComponents(c) for c in comps]
                for k, comps in components.items()
            }
        return super().instantiate(**kwargs)
