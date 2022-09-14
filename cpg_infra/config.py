import dataclasses
from enum import Enum

DOMAIN = "populationgenomics.org.au"


class DeserializableDataclass:
    def __post_init__(self):
        """
        Do correct initialization of subclasses where appropriate
        """
        fields = {field.name: field.type for field in dataclasses.fields(type(self))}

        from types import UnionType

        for fieldname, ftype in fields.items():
            value = self.__dict__.get(fieldname)
            if not value:
                continue
            dtypes = []
            if isinstance(ftype, UnionType):
                is_already_correct_type = False
                for dtype in ftype.__args__:
                    if dtype and issubclass(dtype, DeserializableDataclass):
                        dtypes.append(dtype)
                    elif dtype and isinstance(value, dtype):
                        is_already_correct_type = True
                if is_already_correct_type:
                    continue

            elif issubclass(ftype, DeserializableDataclass):
                dtypes.append(ftype)

            e = None
            for dtype in dtypes:
                if not isinstance(value, dict):
                    raise ValueError('Expected ')
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
    @dataclasses.dataclass(frozen=True)
    class GCP(DeserializableDataclass):
        customer_id: str
        common_artifact_registry_project: str
        common_artifact_registry_name: str
        reference_bucket_name: str
        config_bucket_name: str

    @dataclasses.dataclass(frozen=True)
    class Azure(DeserializableDataclass):
        subscription_id: str

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

    gcp: GCP | None
    hail: Hail | None
    analysis_runner: AnalysisRunner | None
    web_service: WebService
    notebooks: Notebooks | None
    cromwell: Cromwell | None
    sample_metadata: SampleMetadata | None

    # temporary
    access_group_cache: AccessGroupCache

    azure: Azure | None = None
    
    @staticmethod
    def from_toml(path):
        import toml

        with open(path) as f:
            d = toml.load(f)
        return CPGInfrastructureConfig.from_dict(d)

    @staticmethod
    def from_dict(d):
        if 'infrastructure' in d:
            d = d['infrastructure']
        return CPGInfrastructureConfig(**d)


class CPGDatasetComponents(Enum):
    STORAGE = "storage"
    SPARK = "spark"
    CROMWELL = "cromwell"
    NOTEBOOKS = "notebooks"
    HAIL_ACCOUNTS = "hail-accounts"
    SAMPLE_METADATA = "sample_metadata"
    CONTAINER_REGISTRY = 'container-registry'
    ANALYSIS_RUNNER = 'analysis-runner'

    @staticmethod
    def default_component_for_infrastructure():

        return {
            "dev": list(CPGDatasetComponents),
            "gcp": list(CPGDatasetComponents),
            "azure": [
                CPGDatasetComponents.STORAGE,
                # CPGDatasetComponents.HAIL_ACCOUNTS,
                # CPGDatasetComponents.SAMPLE_METADATA,
            ],
        }


@dataclasses.dataclass(frozen=True)
class CPGDatasetConfig(DeserializableDataclass):
    # duh
    dataset: str

    # hail accounts
    hail_service_account_test: str
    hail_service_account_standard: str
    hail_service_account_full: str

    deployment_service_account_test: str | None = None
    deployment_service_account_standard: str | None = None
    deployment_service_account_full: str | None = None

    deploy_locations: list[str] = dataclasses.field(default_factory=lambda: ['gcp'])

    # creates a release requester-pays bucket
    enable_release: bool = False

    # give access for this dataset to access any other it depends on
    depends_on: list[str] = dataclasses.field(default_factory=list)

    # extra places that collaborators can upload data too
    additional_upload_buckets: list[str] = dataclasses.field(default_factory=list)

    # convenience place for plumbing extra service-accounts for SM
    sm_read_only_sas: list[str] = dataclasses.field(default_factory=list)
    sm_read_write_sas: list[str] = dataclasses.field(default_factory=list)

    components: dict[str, list[CPGDatasetComponents]] = dataclasses.field(
        default_factory=dict
    )

    archive_age: int = 30

    @classmethod
    def from_pulumi(cls, config, **kwargs):
        fields = {field.name: field.type for field in dataclasses.fields(cls)}
        d = {**kwargs}
        for fieldname, ftype in fields.items():

            if any(str(ftype).startswith(ext + "[") for ext in ("list", "dict")):
                value = config.get_object(fieldname)
            elif ftype == bool:
                value = config.get_bool(fieldname)
            else:
                value = config.get(fieldname)
                if value:
                    value = ftype(value)

            if value:
                d[fieldname] = value

        return cls(**d)
