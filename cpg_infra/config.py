import dataclasses
from enum import Enum

DOMAIN = "populationgenomics.org.au"


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
                CPGDatasetComponents.HAIL_ACCOUNTS,
                # CPGDatasetComponents.SAMPLE_METADATA,
            ],
        }


@dataclasses.dataclass(frozen=True)
class CPGDatasetConfig:
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
