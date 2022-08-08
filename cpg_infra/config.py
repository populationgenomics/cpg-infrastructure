import dataclasses


@dataclasses.dataclass
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

    # creates a release requester-pays bucket
    enable_release: bool = False

    # give access for this dataset to access any other it depends on
    depends_on: list[str] = dataclasses.field(default_factory=list)

    # extra places that collaborators can upload data too
    additional_upload_buckets: list[str] = dataclasses.field(default_factory=list)

    # convenience place for plumbing extra service-accounts for SM
    sm_read_only_sas: list[str] = dataclasses.field(default_factory=list)
    sm_read_write_sas: list[str] = dataclasses.field(default_factory=list)

    archive_age: int = 30
