from typing import Type, Any, Iterator
from inspect import isclass
from collections import defaultdict, namedtuple

from functools import lru_cache

import pulumi

from cpg_infra.abstraction.azure import AzureInfra
from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.abstraction.base import CloudInfraBase, DevInfra
from cpg_infra.config import CPGDatasetConfig

DOMAIN = "populationgenomics.org.au"
CUSTOMER_ID = "C010ys3gt"
REGION = "australia-southeast1"
ANALYSIS_RUNNER_PROJECT = "analysis-runner"
CPG_COMMON_PROJECT = "cpg-common"
ANALYSIS_RUNNER_SERVICE_ACCOUNT = (
    "analysis-runner-server@analysis-runner.iam.gserviceaccount.com"
)
ANALYSIS_RUNNER_LOGGER_SERVICE_ACCOUNT = (
    "sample-metadata@analysis-runner.iam.gserviceaccount.com"
)
WEB_SERVER_SERVICE_ACCOUNT = "web-server@analysis-runner.iam.gserviceaccount.com"
ACCESS_GROUP_CACHE_SERVICE_ACCOUNT = (
    "access-group-cache@analysis-runner.iam.gserviceaccount.com"
)
REFERENCE_BUCKET_NAME = "cpg-reference"
ANALYSIS_RUNNER_CONFIG_BUCKET_NAME = "cpg-config"
HAIL_WHEEL_BUCKET_NAME = "cpg-hail-ci"
NOTEBOOKS_PROJECT = "notebooks-314505"
# cromwell-submission-access@populationgenomics.org.au
CROMWELL_ACCESS_GROUP_ID = "groups/03cqmetx2922fyu"
CROMWELL_RUNNER_ACCOUNT = "cromwell-runner@cromwell-305305.iam.gserviceaccount.com"
SAMPLE_METADATA_PROJECT = "sample-metadata"
SAMPLE_METADATA_API_SERVICE_ACCOUNT = (
    "sample-metadata-api@sample-metadata.iam.gserviceaccount.com"
)
ACCESS_LEVELS = ("test", "standard", "full")
TMP_BUCKET_PERIOD_IN_DAYS = 8  # tmp content gets deleted afterwards.

SampleMetadataAccessorMembership = namedtuple(
    # the member_key for a group might be group.group_key.id
    "SampleMetadataAccessorMembership",
    ["name", "member_key", "permissions"],
)

class CPGInfrastructure:
    def __init__(self, infra, config: CPGDatasetConfig):
        self.config: CPGDatasetConfig = config
        self.infra: CloudInfraBase = infra(config) if isclass(infra) else infra

        # cache
        self._working_machine_accounts_by_access_level = None

    def main(self):

        main_upload_account = self.infra.create_machine_account("main-upload")
        main_upload_buckets = self.get_main_upload_buckets()

        test_upload_bucket = self.infra.create_bucket("test-upload", lifecycle_rules=[])
        for bname, main_upload_bucket in main_upload_buckets.items():
            self.infra.add_member_to_bucket(
                f"main-upload-service-account-{bname}-bucket-creator",
                bucket=main_upload_bucket,
                member=main_upload_account,
            )

        working_machine_accounts = self.get_working_machine_accounts_by_type()
        for obj in self.working_machine_accounts_gen():
            print(obj)

    @lru_cache()
    def get_working_machine_accounts_by_type(self) -> dict[str, list[tuple[str, Any]]]:
        print('GENERATING ACCOUNTS')
        machine_accounts: dict[str, list] = defaultdict(list)
        for kind, access_level_and_sa in self.get_hail_accounts().items():
            machine_accounts[kind].extend(access_level_and_sa)
        for kind, access_level_and_sa in self.generate_dataproc_and_cromwell_machine_accounts().items():
            machine_accounts[kind].extend(access_level_and_sa)

        return machine_accounts

    def working_machine_accounts_gen(self) -> Iterator[tuple[str, str, Any]]:
        for kind, values in self.get_working_machine_accounts_by_type().items():
            for access_level, service_account in values:
                yield kind, access_level, service_account

    def get_hail_accounts(self):
        service_accounts = defaultdict(list)
        for kind in "hail", "deployment":
            for access_level in ACCESS_LEVELS:
                service_account = getattr(
                    self.config, f"{kind}_service_account_{access_level}"
                )
                if service_account:
                    service_accounts[kind].append((access_level, service_account))

        return service_accounts

    def generate_dataproc_and_cromwell_machine_accounts(self):
        service_accounts = defaultdict(list)
        for kind in "dataproc", "cromwell":
            for access_level in ACCESS_LEVELS:
                service_accounts[kind].append(
                    (
                        access_level,
                        self.infra.create_machine_account(f"{kind}-{access_level}"),
                    )
                )

        return service_accounts

    def get_main_upload_buckets(self) -> dict[str, Any]:
        main_upload_undelete = self.infra.rule_undelete(days=30)
        main_upload_buckets = {
            "main-upload": self.infra.create_bucket(
                "main-upload", lifecycle_rules=[main_upload_undelete]
            )
        }

        for additional_upload_bucket in self.config.additional_upload_buckets:
            main_upload_buckets[additional_upload_bucket] = self.infra.create_bucket(
                additional_upload_bucket,
                lifecycle_rules=[main_upload_undelete],
                unique=True,
            )

        return main_upload_buckets


if __name__ == "__main__":

    class MyMocks(pulumi.runtime.Mocks):
        def new_resource(self, args: pulumi.runtime.MockResourceArgs):
            return [args.name + "_id", args.inputs]

        def call(self, args: pulumi.runtime.MockCallArgs):
            return {}

    pulumi.runtime.set_mocks(
        MyMocks(),
        preview=False,  # Sets the flag `dry_run`, which is true at runtime during a preview.
    )

    _infras: list[Type[CloudInfraBase]] = [
        DevInfra,
        # GcpInfrastructure,
        # AzureInfra,
    ]

    for _infra in _infras:

        _config = CPGDatasetConfig(
            **{
                "dataset": "fewgenomes",
                "hail_service_account_test": "fewgenomes-test@service-account",
                "hail_service_account_standard": "fewgenomes-standard@service-account",
                "hail_service_account_full": "fewgenomes-full@service-account",
            }
        )
        CPGInfrastructure(_infra, _config).main()
