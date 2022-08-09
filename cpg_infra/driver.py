import re
from typing import Type, Any, Iterator, Iterable
from inspect import isclass
from collections import defaultdict, namedtuple

from functools import lru_cache

import pulumi

from cpg_infra.abstraction.azure import AzureInfra
from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.abstraction.base import (
    CloudInfraBase,
    DevInfra,
    SecretMembership,
    BucketPermission,
)
from cpg_infra.config import CPGDatasetConfig, CPGDatasetComponents

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
WEB_SERVER_SERVICE_ACCOUNT = (
    "serviceAccount:web-server@analysis-runner.iam.gserviceaccount.com"
)
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
TMP_BUCKET_PERIOD_IN_DAYS = 8  # tmp content gets deleted afterwards.

SampleMetadataAccessorMembership = namedtuple(
    # the member_key for a group might be group.group_key.id
    "SampleMetadataAccessorMembership",
    ["name", "member_key", "permissions"],
)

SM_TEST_READ = "test-read"
SM_TEST_WRITE = "test-write"
SM_MAIN_READ = "main-read"
SM_MAIN_WRITE = "main-write"
SAMPLE_METADATA_PERMISSIONS = [
    SM_TEST_READ,
    SM_TEST_WRITE,
    SM_MAIN_READ,
    SM_MAIN_WRITE,
]


AccessLevel = str
ACCESS_LEVELS: Iterable[AccessLevel] = ("test", "standard", "full")
NON_NAME_REGEX = re.compile(r"[^A-Za-z0-9_-]")


class CPGInfrastructure:
    @staticmethod
    def deploy_all_from_config(config: CPGDatasetConfig):
        _infra_map = {c.name(): c for c in CloudInfraBase.__subclasses__()}
        _infras: list[Type[CloudInfraBase]] = [
            _infra_map[n] for n in config.deploy_locations
        ]

        for _infra in _infras:
            CPGInfrastructure(_infra, config).main()

    def __init__(self, infra, config: CPGDatasetConfig):
        self.config: CPGDatasetConfig = config
        self.infra: CloudInfraBase = infra(self.config) if isclass(infra) else infra
        self.components: list[CPGDatasetComponents] = config.components.get(
            self.infra.name(),
            CPGDatasetComponents.default_component_for_infrastructure()[
                self.infra.name()
            ],
        )

    def create_group(self, name: str):
        group_name = f'{self.config.dataset}-{name}'
        group = self.infra.create_group(group_name)
        self.infra.add_group_member(
            f"access-group-cacher-{group_name}",
            group,
            ACCESS_GROUP_CACHE_SERVICE_ACCOUNT,
        )
        return group

    def main(self):

        # set-up machine group membership
        self.setup_access_level_group_memberships()
        self.setup_dependent_group_memberships()

        # setup bucket permissions
        if CPGDatasetComponents.STORAGE in self.components:
            self.setup_main_bucket_permissions()
            self.setup_main_tmp_bucket()
            self.setup_main_analysis_bucket()
            self.setup_main_web_bucket_permissions()
            self.setup_main_upload_buckets_permissions()
            self.setup_test_buckets_permissions()

            if self.config.enable_release:
                self.setup_release_bucket_permissions()

        if CPGDatasetComponents.SAMPLE_METADATA in self.components:

            # sample-metadata
            self.setup_sample_metadata_permissions()

        if CPGDatasetComponents.CROMWELL in self.components:
            self.setup_cromwell_machine_accounts()

        if isinstance(self.infra, GcpInfrastructure):
            self._gcp_extra_steps()

        # outputs
        self.setup_access_level_group_outputs()

    # region GCP SPECIFIC

    def _gcp_extra_steps(self):
        if 'azure' in self.config.deploy_locations:
            # we'll set up GCP service-accounts for use by Azure SAs, with credentials on SM
            pass

        pass

    # endregion GCP SPECIFIC
    # region MACHINE ACCOUNTS

    @property
    @lru_cache()
    def main_upload_account(self):
        return self.infra.create_machine_account("main-upload")

    @property
    @lru_cache()
    def notebook_account(self):
        pass

    @property
    @lru_cache()
    def working_machine_accounts_by_type(
        self,
    ) -> dict[str, list[tuple[AccessLevel, Any]]]:
        print("GENERATING ACCOUNTS")
        machine_accounts: dict[str, list] = defaultdict(list)
        for kind, access_level_and_sa in self._get_hail_and_deploy_accounts().items():
            machine_accounts[kind].extend(access_level_and_sa)
        for (
            kind,
            access_level_and_sa,
        ) in self._generate_dataproc_and_cromwell_machine_accounts().items():
            machine_accounts[kind].extend(access_level_and_sa)

        return machine_accounts

    def working_machine_accounts_kind_al_account_gen(
        self,
    ) -> Iterator[tuple[str, AccessLevel, Any]]:
        for kind, values in self.working_machine_accounts_by_type.items():
            for access_level, machine_account in values:
                yield kind, access_level, machine_account

    def working_machine_accounts_by_access_level(self):
        machine_accounts: dict[AccessLevel, list[any]] = defaultdict(list)
        for kind, values in self.working_machine_accounts_by_type.items():
            for access_level, machine_account in values:
                machine_accounts[access_level].append(machine_account)

        return machine_accounts

    def _get_hail_and_deploy_accounts(self) -> dict[str, list[tuple[AccessLevel, Any]]]:
        service_accounts = defaultdict(list)
        for kind in "hail", "deployment":
            for access_level in ACCESS_LEVELS:
                service_account = getattr(
                    self.config, f"{kind}_service_account_{access_level}"
                )
                if service_account:
                    service_accounts[kind].append((access_level, service_account))

        return service_accounts

    def _generate_dataproc_and_cromwell_machine_accounts(
        self,
    ) -> dict[str, list[tuple[AccessLevel, Any]]]:
        service_accounts = defaultdict(list)
        kinds = []
        if CPGDatasetComponents.SPARK in self.components:
            kinds.append('dataproc')
        if CPGDatasetComponents.CROMWELL in self.components:
            kinds.append('cromwell')

        for kind in kinds:
            for access_level in ACCESS_LEVELS:
                service_accounts[kind].append(
                    (
                        access_level,
                        self.infra.create_machine_account(f"{kind}-sa-{access_level}"),
                    )
                )

        return service_accounts

    # endregion MACHINE ACCOUNTS
    # region GROUPS

    @property
    @lru_cache
    def access_group(self):
        return self.create_group("access")

    @property
    @lru_cache()
    def web_access_group(self):
        return self.create_group("web-access")

    @property
    @lru_cache()
    def release_access_group(self):
        return self.create_group('release-access')

    @property
    @lru_cache()
    def access_level_groups(self) -> dict[AccessLevel, Any]:
        return {al: self.create_group(al) for al in ACCESS_LEVELS}

    @staticmethod
    def get_access_level_group_output_name(*, access_level: AccessLevel):
        return f"{access_level}-access-group-id"

    def setup_access_level_group_outputs(self):
        for access_level, group in self.access_level_groups.items():
            pulumi.export(
                self.get_access_level_group_output_name(access_level=access_level),
                group.id if hasattr(group, "id") else group,
            )

    def setup_access_level_group_memberships(self):
        for (
            kind,
            access_level,
            machine_account,
        ) in self.working_machine_accounts_kind_al_account_gen():
            group = self.access_level_groups[access_level]
            self.infra.add_group_member(
                f"{kind}-{access_level}-access-level-group-membership",
                group=group,
                member=machine_account,
            )

    @property
    @lru_cache()
    def sample_metadata_groups(self) -> dict[str, any]:
        sm_groups = {}
        for key in SAMPLE_METADATA_PERMISSIONS:
            sm_groups[key] = self.create_group(f"sample-metadata-{key}")

        return sm_groups

    def setup_sample_metadata_permissions(self):
        sm_access_levels: list[SampleMetadataAccessorMembership] = [
            SampleMetadataAccessorMembership(
                name="human",
                member_key=self.access_group,
                permissions=(SM_MAIN_READ, SM_TEST_READ, SM_TEST_WRITE),
            ),
            SampleMetadataAccessorMembership(
                name="test",
                member_key=self.access_level_groups["test"],
                permissions=(SM_MAIN_READ, SM_TEST_READ, SM_TEST_WRITE),
            ),
            SampleMetadataAccessorMembership(
                name="standard",
                member_key=self.access_level_groups["standard"],
                permissions=(SM_MAIN_READ, SM_MAIN_WRITE),
            ),
            SampleMetadataAccessorMembership(
                name="full",
                member_key=self.access_level_groups["full"],
                permissions=SAMPLE_METADATA_PERMISSIONS,
            ),
            # allow the analysis-runner logging cloud function to update the sample-metadata project
            SampleMetadataAccessorMembership(
                name="analysis-runner-logger",
                member_key=ANALYSIS_RUNNER_LOGGER_SERVICE_ACCOUNT,
                permissions=SAMPLE_METADATA_PERMISSIONS,
            ),
        ]

        # extra custom SAs
        extra_sm_read_sas = self.config.sm_read_only_sas
        extra_sm_write_sas = self.config.sm_read_write_sas

        for sa in extra_sm_read_sas:
            sm_access_levels.append(
                SampleMetadataAccessorMembership(
                    name=self._get_name_from_external_sa(sa),
                    member_key=sa,
                    permissions=(SM_MAIN_READ,),
                )
            )
        for sa in extra_sm_write_sas:
            sm_access_levels.append(
                SampleMetadataAccessorMembership(
                    name=self._get_name_from_external_sa(sa),
                    member_key=sa,
                    permissions=(SM_MAIN_READ, SM_MAIN_WRITE),
                )
            )

        # give access to sample_metadata groups (and hence sample-metadata API through secrets)
        for name, member, permission in sm_access_levels:
            for kind in permission:
                self.infra.add_group_member(
                    f"sample-metadata-{kind}-{name}-access-level-group-membership",
                    self.sample_metadata_groups[kind],
                    member,
                )

    def setup_sample_metadata_members_cache_secrets(self):
        # hopefully this is just temporary
        for sm_group_name, sm_group in self.sample_metadata_groups.items():
            self.infra.add_group_member(
                f"sample-metadata-group-cache-{sm_group_name}-group-membership",
                sm_group,
                ACCESS_GROUP_CACHE_SERVICE_ACCOUNT,
            )

            sm_cache_secret = self.infra.create_secret(f"{sm_group_name}-members-cache")

            self.infra.add_secret_member(
                f"{sm_group_name}-group-cache-secret-accessor",
                sm_cache_secret,
                ACCESS_GROUP_CACHE_SERVICE_ACCOUNT,
                SecretMembership.ADMIN,
            ),
            self.infra.add_secret_member(
                f"{sm_group_name}-smservice-secret-accessor",
                sm_cache_secret,
                SAMPLE_METADATA_API_SERVICE_ACCOUNT,
                SecretMembership.ACCESSOR,
            ),

    # endregion GROUPS
    # region MAIN BUCKETS

    def setup_main_bucket_permissions(self):
        # access has list permission

        self.infra.add_member_to_bucket(
            "project-bucket-lister",
            self.main_bucket,
            self.access_group,
            BucketPermission.LIST,
        )

        self.infra.add_member_to_bucket(
            "standard-main-bucket-view-create",
            self.main_bucket,
            self.access_level_groups["standard"],
            BucketPermission.APPEND,
        )

        self.infra.add_member_to_bucket(
            "full-main-bucket-admin",
            self.main_bucket,
            self.access_level_groups["full"],
            BucketPermission.MUTATE,
        )

    def setup_main_tmp_bucket(self):
        self.infra.add_member_to_bucket(
            "standard-main-tmp-bucket-view-create",
            self.main_tmp_bucket,
            self.access_level_groups["standard"],
            BucketPermission.APPEND,
        )

        self.infra.add_member_to_bucket(
            "full-main-tmp-bucket-admin",
            self.main_tmp_bucket,
            self.access_level_groups["full"],
            BucketPermission.MUTATE,
        )

    def setup_main_analysis_bucket(self):
        self.infra.add_member_to_bucket(
            "access-group-main-analysis-bucket-viewer",
            self.main_analysis_bucket,
            self.access_group,
            BucketPermission.READ,
        )
        self.infra.add_member_to_bucket(
            "standard-main-analysis-bucket-view-create",
            self.main_analysis_bucket,
            self.access_level_groups["standard"],
            BucketPermission.APPEND,
        )

        self.infra.add_member_to_bucket(
            "full-main-analysis-bucket-admin",
            self.main_analysis_bucket,
            self.access_level_groups["full"],
            BucketPermission.MUTATE,
        )

    def setup_main_web_bucket_permissions(self):
        self.infra.add_member_to_bucket(
            "access-group-main-web-bucket-viewer",
            self.main_web_bucket,
            self.access_group,
            BucketPermission.READ,
        )

        # web-server
        self.infra.add_member_to_bucket(
            "web-server-main-web-bucket-viewer",
            self.main_web_bucket,
            WEB_SERVER_SERVICE_ACCOUNT,
            BucketPermission.READ,
        )

        self.infra.add_member_to_bucket(
            "standard-main-web-bucket-view-create",
            self.main_web_bucket,
            self.access_level_groups["standard"],
            BucketPermission.APPEND,
        )

        self.infra.add_member_to_bucket(
            "full-main-web-bucket-admin",
            self.main_web_bucket,
            self.access_level_groups["full"],
            BucketPermission.APPEND,
        )

    def setup_main_upload_buckets_permissions(self):
        for bname, main_upload_bucket in self.main_upload_buckets.items():
            self.infra.add_member_to_bucket(
                f"main-upload-service-account-{bname}-bucket-creator",
                bucket=main_upload_bucket,
                member=self.main_upload_account,
                membership=BucketPermission.MUTATE,
            )

    @property
    @lru_cache()
    def main_bucket(self):
        return self.infra.create_bucket(
            "main", lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @property
    @lru_cache()
    def main_tmp_bucket(self):
        return self.infra.create_bucket(
            "main-tmp",
            lifecycle_rules=[self.infra.bucket_rule_temporary()],
            versioning=False,
        )

    @property
    @lru_cache()
    def main_analysis_bucket(self):
        return self.infra.create_bucket(
            "main-analysis", lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @property
    @lru_cache()
    def main_web_bucket(self):
        return self.infra.create_bucket(
            "main-web", lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @property
    @lru_cache()
    def main_upload_buckets(self) -> dict[str, Any]:
        main_upload_undelete = self.infra.bucket_rule_undelete(days=30)
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

    # endregion MAIN BUCKETS
    # region TEST BUCKETS

    def setup_test_buckets_permissions(self):
        """
        Test bucket permissions are much more uniform,
        so just work out some more generic mechanism
        """

        buckets = [
            ('test', self.test_bucket),
            ('test-analysis', self.test_analysis_bucket),
            ('test-tmp', self.test_tmp_bucket),
            ('test-web', self.test_web_bucket),
        ]

        for bucket_name, bucket in buckets:

            test_bucket_admins: list[tuple[str, Any]] = [
                (f'access-group-{bucket_name}-bucket-admin', self.access_group),
                *[
                    (f"{access_level}-{bucket_name}-bucket-admin", group)
                    for access_level, group in self.access_level_groups.items()
                ],
            ]
            for resource_key, group in test_bucket_admins:
                self.infra.add_member_to_bucket(
                    resource_key,
                    bucket,
                    group,
                    BucketPermission.MUTATE,
                )

    @property
    @lru_cache()
    def test_bucket(self):
        return self.infra.create_bucket(
            "test", lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @property
    @lru_cache()
    def test_analysis_bucket(self):
        return self.infra.create_bucket(
            "test-analysis", lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @property
    @lru_cache()
    def test_web_bucket(self):
        return self.infra.create_bucket(
            "test-web", lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @property
    @lru_cache()
    def test_tmp_bucket(self):
        return self.infra.create_bucket(
            "test-tmp",
            lifecycle_rules=[self.infra.bucket_rule_temporary()],
            versioning=False,
        )

    # endregion TEST BUCKETS
    # region RELEASE BUCKETS

    def setup_release_bucket_permissions(self):
        self.infra.add_member_to_bucket(
            'access-group-release-bucket-viewer',
            self.release_bucket,
            self.access_group,
            BucketPermission.READ,
        )

        self.infra.add_member_to_bucket(
            'release-access-group-release-bucket-viewer',
            self.release_bucket,
            self.release_access_group,
            BucketPermission.READ,
        )

        self.infra.add_member_to_bucket(
            'full-release-bucket-admin',
            self.release_bucket,
            self.access_level_groups['full'],
            BucketPermission.MUTATE,
        )

    @property
    @lru_cache()
    def release_bucket(self):
        return self.infra.create_bucket(
            'release-requester-pays',
            lifecycle_rules=[self.infra.bucket_rule_undelete()],
        )

    # endregion RELEASE BUCKETS
    # region CROMWELL

    def setup_cromwell_machine_accounts(self):
        cromwell_machine_accounts = self.working_machine_accounts_by_type.get(
            'cromwell'
        )
        if not cromwell_machine_accounts:
            raise ValueError(
                'This method may be called where cromwell machine accounts are not activated'
            )

        for access_level, machine_account in cromwell_machine_accounts:

            # To use a service account for VMs, Cromwell accounts need
            # to be allowed to use themselves ;)
            self.infra.add_member_to_machine_account_access(
                f'cromwell-service-account-{access_level}-service-account-user',
                machine_account,
                machine_account,
            )

            # TODO: test if this is necessary, I don't think it should be :suss:
            # Allow the Cromwell SERVER to run worker VMs using the Cromwell SAs
            self.infra.add_member_to_machine_account_access(
                f'cromwell-runner-{access_level}-service-account-user',
                machine_account,
                CROMWELL_RUNNER_ACCOUNT,
            )

    def _GCP_setup_cromwell(self):
        assert isinstance(self.infra, GcpInfrastructure)
        # Allow the Cromwell service accounts to run workflows.

    # endregion CROMWELL
    # region DEPENDENCIES

    def setup_dependent_group_memberships(self):
        for access_level, primary_access_group in self.access_level_groups.items():
            for dependency in self.config.depends_on:
                dependency_group_id = self.get_pulumi_stack(dependency).get_output(
                    self.get_access_level_group_output_name(access_level=access_level),
                )

                # add this dataset to dependencies membership
                self.infra.add_group_member(
                    f"{dependency}-{access_level}-access-level-group",
                    dependency_group_id,
                    primary_access_group,
                )

    @staticmethod
    @lru_cache()
    def get_pulumi_stack(dependency_name: str):
        return pulumi.StackReference(dependency_name)

    # endregion DEPENDENCIES
    # region UTILS
    @staticmethod
    def _get_name_from_external_sa(email: str, suffix=".iam.gserviceaccount.com"):
        """
        Convert service account email to name + some filtering.

        >>> CPGInfrastructure._get_name_from_external_sa('my-service-account@project.iam.gserviceaccount.com')
        'my-service-account-project'

        >>> CPGInfrastructure._get_name_from_external_sa('yourname@populationgenomics.org.au')
        'yourname'

        >>> CPGInfrastructure._get_name_from_external_sa('my.service-account+extra@domain.com')
        'my-service-account-extra'
        """
        if email.endswith(suffix):
            base = email[: -len(suffix)]
        else:
            base = email.split("@")[0]

        return NON_NAME_REGEX.sub("-", base).replace("--", "-")

    # endregion UTILS


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
