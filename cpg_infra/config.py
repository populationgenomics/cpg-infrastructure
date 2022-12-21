# pylint: disable=missing-function-docstring,missing-class-docstring,invalid-name,too-many-return-statements
"""
This module contains all the configuration objects that are used to
describe the CPG infrastructure, including what's required from a
specific dataset.
"""

import dataclasses
from enum import Enum
from types import UnionType
from typing import get_origin, get_args
import toml


# If we serialize the pulumi configurations + any other TOMLs
# we can tell quickly if the configuration is correct and complete
# and make it simpler for tasks to use the correct keys.
# We have a __post_init__ here to ensure that subdictionaries are
# parsed into the structure we want, and because the python.dataclasses
# won't do that automatically for us :(
class DeserializableDataclass:
    def __post_init__(self):
        """
        Do correct initialization of subclasses where appropriate
        """
        fields = {field.name: field.type for field in dataclasses.fields(type(self))}

        for fieldname, ftype in fields.items():
            value = self.__dict__.get(fieldname)
            if not value:
                continue
            dtypes = []
            # determine which type we should try to parse the value as
            # handle unions (eg: None | DType)
            if isinstance(ftype, UnionType):
                is_already_correct_type = False
                for dtype in get_args(ftype):
                    if dtype and issubclass(dtype, DeserializableDataclass):
                        # It's a DeserializableDataclass :)
                        dtypes.append(dtype)
                    elif dtype and isinstance(value, dtype):
                        is_already_correct_type = True
                if is_already_correct_type:
                    continue

            elif issubclass(ftype, DeserializableDataclass):
                dtypes.append(ftype)

            e = None
            # try to see if the value will parse as one of the detected DTypes
            for dtype in dtypes:
                if not isinstance(value, dict):
                    raise ValueError(
                        f'Expected {value} to be a dictionary to parse, got {type(value)}.'
                    )
                try:
                    self.__dict__[fieldname] = dtype(**value)
                    e = None
                    break
                except TypeError as exc:
                    e = exc

            if e:
                raise e


@dataclasses.dataclass(frozen=True)
class CPGInfrastructureConfig(DeserializableDataclass):
    """
    Configuration that describes all variables
    required to instantiate the CPG infrastructure
    """

    @dataclasses.dataclass(frozen=True)
    class GCP(DeserializableDataclass):
        customer_id: str
        region: str
        billing_project_id: str
        billing_account_id: int
        budget_notification_pubsub: str | None
        config_bucket_name: str

    @dataclasses.dataclass(frozen=True)
    class Azure(DeserializableDataclass):
        region: str
        tenant: str

    @dataclasses.dataclass(frozen=True)
    class Hail(DeserializableDataclass):
        @dataclasses.dataclass(frozen=True)
        class GCP(DeserializableDataclass):
            wheel_bucket_name: str

        gcp: GCP

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
        @dataclasses.dataclass(frozen=True)
        class GCP(DeserializableDataclass):
            server_machine_account: str

        gcp: GCP

    # temporary
    @dataclasses.dataclass(frozen=True)
    class AccessGroupCache(DeserializableDataclass):
        process_machine_account: str

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
    class SampleMetadata(DeserializableDataclass):
        @dataclasses.dataclass(frozen=True)
        class GCP(DeserializableDataclass):
            project: str
            service_name: str
            machine_account: str

        gcp: GCP

    domain: str
    dataset_storage_prefix: str
    budget_currency: str
    reference_dataset: str
    web_url_template: str

    config_destination: str

    gcp: GCP | None = None
    azure: Azure | None = None
    hail: Hail | None = None
    analysis_runner: AnalysisRunner | None = None
    web_service: WebService | None = None
    notebooks: Notebooks | None = None
    cromwell: Cromwell | None = None
    sample_metadata: SampleMetadata | None = None

    # temporary
    access_group_cache: AccessGroupCache = None

    # When resources are renamed, it can be useful to explicitly apply changes in two
    # phases: delete followed by create; that's opposite of the default create followed by
    # delete, which can end up with missing permissions. To implement the first phase
    # (delete), simply change this to 'True', then revert to reapply group memberships
    disable_group_memberships: bool = False

    budget_notification_thresholds: list[float] = dataclasses.field(
        default_factory=lambda: [0.5, 0.9, 1.0]
    )

    @staticmethod
    def from_toml(path):
        with open(path, encoding='utf-8') as f:
            d = toml.load(f)
        return CPGInfrastructureConfig.from_dict(d)

    @staticmethod
    def from_dict(d):
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
    def default_component_for_infrastructure():

        return {
            'dry-run': list(CPGDatasetComponents),
            'gcp': list(CPGDatasetComponents),
            'azure': [
                CPGDatasetComponents.STORAGE,
                CPGDatasetComponents.HAIL_ACCOUNTS,
                # CPGDatasetComponents.SAMPLE_METADATA,
            ],
        }


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
                f'Could not instantiate {self.__class__.__name__} for "{self.dataset}": {str(e)}'
            ) from e

    @dataclasses.dataclass(frozen=True)
    class Gcp(DeserializableDataclass):
        project: str
        region: str | None = None

        hail_service_account_test: str = None
        hail_service_account_standard: str = None
        hail_service_account_full: str = None

    @dataclasses.dataclass(frozen=True)
    class Azure(DeserializableDataclass):
        region: str | None = None

        hail_service_account_test: str = None
        hail_service_account_standard: str = None
        hail_service_account_full: str = None

    dataset: str

    gcp: Gcp
    azure: Azure = None

    deployment_service_account_test: str | None = None
    deployment_service_account_standard: str | None = None
    deployment_service_account_full: str | None = None

    create_container_registry: bool = False

    deploy_locations: list[str] = dataclasses.field(default_factory=lambda: ['gcp'])

    # creates a release requester-pays bucket
    enable_release: bool = False
    enable_shared_project: bool = False
    shared_project_budget: int = None
    # give access for this dataset to access any other it depends on
    depends_on: list[str] = dataclasses.field(default_factory=list)

    # extra places that collaborators can upload data too
    additional_upload_buckets: list[str] = dataclasses.field(default_factory=list)

    # convenience place for plumbing extra service-accounts for SM
    sm_read_only_sas: list[str] = dataclasses.field(default_factory=list)
    sm_read_write_sas: list[str] = dataclasses.field(default_factory=list)
    archive_age: int = 30

    components: dict[str, list[CPGDatasetComponents]] = dataclasses.field(
        default_factory=dict
    )

    @classmethod
    def instantiate(cls, **kwargs):
        if components := kwargs.get('components'):
            kwargs['components'] = {
                k: [CPGDatasetComponents(c) for c in comps]
                for k, comps in components.items()
            }
        return cls(**kwargs)

    @classmethod
    def from_pulumi(cls, config, **kwargs):
        """
        From a pulumi config, construct this class.
        This will call specific get_bool, get_object, get methods where appropriate
        """
        fields = {field.name: field.type for field in dataclasses.fields(cls)}
        d = {**kwargs}
        for fieldname, ftype in fields.items():
            value = parse_value_from_type(config, fieldname, ftype)
            if value:
                d[fieldname] = value

        if 'components' in d:
            d['components'] = {
                k: [CPGDatasetComponents(c) for c in comps]
                for k, comps in d['components'].items()
            }

        return cls(**d)


def parse_value_from_type(config, fieldname, ftype):
    if ftype is None:
        return None

    if ftype in (list, dict) or get_origin(ftype) in (list, dict):
        ftype_type = ftype if ftype in (list, dict) else get_origin(ftype)
        value = config.get_object(fieldname)

        if value and isinstance(value, ftype_type):
            return value
        if value:
            print(
                f'{fieldname} :: {value} ({type(value)}) was parsed, but was not of type {ftype}'
            )

        return None

    if isinstance(ftype, UnionType) == UnionType:
        for inner_type in get_args(ftype):
            value = parse_value_from_type(config, fieldname, inner_type)
            if value:
                return value

        return None

    if ftype == bool:
        return config.get_bool(fieldname)

    value = config.get(fieldname)
    if value is None:
        return value

    inner_types = get_args(ftype)
    if inner_types:
        for inner_type in inner_types:
            value = parse_value_from_type(config, fieldname, inner_type)
            if value:
                return value
    else:
        try:
            value = ftype(value)
            if value:
                return value
        except (ValueError, TypeError):
            pass

    return None
