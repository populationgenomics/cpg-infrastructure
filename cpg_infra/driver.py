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
SAMPLE_METADATA_SERVICE_NAME = 'sample-metadata-api'
SAMPLE_METADATA_API_SERVICE_ACCOUNT = (
    "serviceAccount:sample-metadata-api@sample-metadata.iam.gserviceaccount.com"
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

        self.should_setup_storage = CPGDatasetComponents.STORAGE in self.components
        self.should_setup_spark = CPGDatasetComponents.SPARK in self.components
        self.should_setup_cromwell = CPGDatasetComponents.CROMWELL in self.components
        self.should_setup_notebooks = CPGDatasetComponents.NOTEBOOKS in self.components
        self.should_setup_sample_metadata = (
            CPGDatasetComponents.SAMPLE_METADATA in self.components
        )
        self.should_setup_hail = CPGDatasetComponents.HAIL_ACCOUNTS in self.components
        self.should_setup_container_registry = (
            CPGDatasetComponents.CONTAINER_REGISTRY in self.components
        )

    def create_group(self, name: str):
        group_name = f'{self.config.dataset}-{name}'
        group = self.infra.create_group(group_name)
        return group

    def main(self):

        # access-groups
        self.setup_access_groups()

        # optional components

        if self.should_setup_storage:
            self.setup_storage()
        if self.should_setup_sample_metadata:
            self.setup_cromwell()
        if self.should_setup_hail:
            self.setup_hail()
        if self.should_setup_cromwell:
            self.setup_cromwell_machine_accounts()
        if self.should_setup_spark:
            self.setup_spark()
        if self.should_setup_notebooks:
            self.setup_notebooks()

        self.setup_group_caches()

        # outputs

    # region MACHINE ACCOUNTS

    @property
    @lru_cache()
    def main_upload_account(self):
        return self.infra.create_machine_account("main-upload")

    @property
    @lru_cache()
    def working_machine_accounts_by_type(
        self,
    ) -> dict[str, list[tuple[AccessLevel, Any]]]:
        print("GENERATING ACCOUNTS")
        machine_accounts: dict[str, list] = defaultdict(list)

        for access_level, account in self.hail_accounts_by_access_level.items():
            machine_accounts['hail'].append((access_level, account))
        for access_level, account in self.deployment_accounts_by_access_level.items():
            machine_accounts['deployment'].append((access_level, account))
        for access_level, account in self.dataproc_machine_accounts_by_access_level.items():
            machine_accounts['dataproc'].append((access_level, account))
        for access_level, account in self.cromwell_machine_accounts_by_access_level.items():
            machine_accounts['cromwell'].append((access_level, account))

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

    @property
    @lru_cache()
    def deployment_accounts_by_access_level(self):
        accounts = {
            'test': self.config.deployment_service_account_test,
            'standard': self.config.deployment_service_account_standard,
            'full': self.config.deployment_service_account_full,
        }
        if any(ac is None for ac in accounts.values()):
            return {}
        return accounts

    # endregion MACHINE ACCOUNTS
    # region ACCESS GROUPS

    def setup_access_groups(self):
        self.setup_access_level_group_memberships()
        self.setup_dependent_group_memberships()
        self.setup_access_level_group_outputs()

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

    # endregion ACCESS GROUPS
    # region STORAGE

    def setup_storage(self):
        if not self.should_setup_storage:
            return

        self.setup_main_bucket_permissions()
        self.setup_main_tmp_bucket()
        self.setup_main_analysis_bucket()
        self.setup_main_web_bucket_permissions()
        self.setup_main_upload_buckets_permissions()
        self.setup_test_buckets_permissions()

        if self.config.enable_release:
            self.setup_release_bucket_permissions()

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
    # endregion STORAGE
    # region HAIL

    def setup_hail(self):
        self.setup_hail_bucket_permissions()

    def setup_hail_bucket_permissions(self):

        for (
            access_level,
            hail_machine_account,
        ) in self.hail_accounts_by_access_level.items():
            # Full access to the Hail Batch bucket.
            self.infra.add_member_to_bucket(
                f'hail-service-account-{access_level}-hail-bucket-admin',
                self.hail_bucket,
                hail_machine_account,
                BucketPermission.MUTATE,
            )

    @property
    @lru_cache()
    def hail_accounts_by_access_level(self):
        if not self.should_setup_hail:
            return {}
        accounts = {
            'test': self.config.hail_service_account_test,
            'standard': self.config.hail_service_account_standard,
            'full': self.config.hail_service_account_full,
        }
        assert all(ac is not None for ac in accounts.values())
        return accounts

    @property
    @lru_cache()
    def hail_bucket(self):
        return self.infra.create_bucket(
            'hail', lifecycle_rules=[self.infra.bucket_rule_temporary()]
        )

    # endregion HAIL
    # region CROMWELL

    def setup_cromwell(self):
        if not self.should_setup_cromwell:
            return

        self.setup_cromwell_machine_accounts()

    def setup_cromwell_machine_accounts(self):

        for (
            access_level,
            machine_account,
        ) in self.cromwell_machine_accounts_by_access_level.items():

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

        if isinstance(self.infra, GcpInfrastructure):
            self._GCP_setup_cromwell()

    @property
    @lru_cache()
    def cromwell_machine_accounts_by_access_level(self) -> dict[AccessLevel, Any]:
        if not self.should_setup_cromwell:
            return {}

        accounts = {
            access_level: self.infra.create_machine_account(f"cromwell-{access_level}")
            for access_level in ACCESS_LEVELS
        }
        return accounts

    def _GCP_setup_cromwell(self):
        assert isinstance(self.infra, GcpInfrastructure)
        # Allow the Cromwell service accounts to run workflows.

        for (
            access_level,
            account,
        ) in self.cromwell_machine_accounts_by_access_level.items():
            self.infra.add_member_to_lifescience_api(
                f'cromwell-service-account-{access_level}-workflows-runner',
                account,
            )

    # endregion CROMWELL
    # region SPARK

    def setup_spark(self):
        if not self.should_setup_spark:
            return

        spark_accounts = self.dataproc_machine_accounts_by_access_level
        for access_level, hail_account in self.hail_accounts_by_access_level.items():
            self.infra.add_member_to_machine_account_access(
                f'hail-service-account-{access_level}-dataproc-service-account-user',
                spark_accounts[access_level],
                hail_account,
            )

        if isinstance(self.infra, GcpInfrastructure):
            for access_level, account in spark_accounts.items():
                self.infra.add_member_to_dataproc_api(
                    f'dataproc-service-account-{access_level}-dataproc-worker', account
                )

    @property
    @lru_cache()
    def dataproc_machine_accounts_by_access_level(self) -> dict[AccessLevel, Any]:
        if not self.should_setup_spark:
            return {}

        accounts = {
            access_level: self.infra.create_machine_account(f"dataproc-{access_level}")
            for access_level in ACCESS_LEVELS
        }
        return accounts

    # endregion SPARK
    # region SAMPLE METADATA

    def setup_sample_metadata(self):
        if not self.should_setup_sample_metadata:
            return {}

        self.setup_sample_metadata_access_permissions()

        if isinstance(self.infra, GcpInfrastructure):
            # do some cloudrun stuff
            self.setup_sample_metadata_cloudrun_permissions()
        elif isinstance(self.infra, AzureInfra):
            # we'll do some custom stuff here :)
            raise NotImplementedError

    @property
    @lru_cache()
    def sample_metadata_groups(self) -> dict[str, any]:
        if not self.should_setup_sample_metadata:
            return {}

        sm_groups = {
            key: self.create_group(f"sample-metadata-{key}")
            for key in SAMPLE_METADATA_PERMISSIONS
        }

        return sm_groups

    def setup_sample_metadata_cloudrun_permissions(self):
        # now we give the sample_metadata_access_group access to cloud-run instance
        assert isinstance(self.infra, GcpInfrastructure)

        for sm_type, group in self.sample_metadata_groups.items():
            self.infra.add_cloudrun_invoker(
                f'sample-metadata-{sm_type}-cloudrun-invoker',
                service=SAMPLE_METADATA_SERVICE_NAME,
                project=SAMPLE_METADATA_PROJECT,
                member=group,
            )

        self.infra.add_cloudrun_invoker(
            f'sample-metadata-access-group-cloudrun-invoker',
            service=SAMPLE_METADATA_SERVICE_NAME,
            project=SAMPLE_METADATA_PROJECT,
            member=self.access_group,
        )

    def setup_sample_metadata_access_permissions(self):
        if not self.should_setup_sample_metadata:
            return
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

    # endregion SAMPLE METADATA
    # region CONTAINER REGISTRY

    def setup_container_registry(self):
        # give pretty much everyone compute-account access to container registry

        self.infra.add_member_to_artifact_registry('')

    # endregion CONTAINER REGISTRY
    # region NOTEBOOKS

    def setup_notebooks(self):
        self.setup_notebook_account()

    def setup_notebook_account(self):

        # allow access group to use notebook account
        self.infra.add_member_to_machine_account_access(
            'notebook-account-users', self.notebook_account, self.access_group
        )

        # Grant the notebook account the same permissions as the access group members.
        self.infra.add_group_member(
            'notebook-service-account-access-group-member',
            self.access_group,
            self.notebook_account,
        )

        if not isinstance(self.infra, GcpInfrastructure):
            # TODO: How to abstract compute.admin on project
            raise NotImplementedError

        self.infra.add_project_role(
            'notebook-account-compute-admin',
            project=NOTEBOOKS_PROJECT,
            role='roles/compute.admin',
            member=self.notebook_account,
        )

    @property
    @lru_cache()
    def notebook_account(self):
        if self.should_setup_notebooks:
            return None
        return self.infra.create_machine_account(
            f'notebook-{self.config.dataset}', project=NOTEBOOKS_PROJECT
        )

    # endregion NOTEBOOKS
    # region ACCESS GROUP CACHE

    def setup_group_caches(self):
        self.setup_access_group_cache()
        self.setup_web_access_group_cache()
        self.setup_sample_metadata_access_secrets()

    def _setup_group_cache_secret(self, group, key, secret_name: str = None):
        self.infra.add_group_member(
            f"{key}-group-cache-membership",
            group,
            ACCESS_GROUP_CACHE_SERVICE_ACCOUNT,
        )
        group_cache_secret = self.infra.create_secret(
            secret_name or f'{key}-group-cache-secret'
        )
        # Modify access_group_cache secret
        self.infra.add_secret_member(
            f'{key}-group-cache-secret-version-manager',
            group_cache_secret,
            ACCESS_GROUP_CACHE_SERVICE_ACCOUNT,
            SecretMembership.ADMIN,
        )

        return group_cache_secret

    def setup_access_group_cache(self):
        # Allow list of access-group
        secret = self._setup_group_cache_secret(self.access_group, 'access')

        # analysis-runner list
        self.infra.add_secret_member(
            'access-group-cache-secret-accessor',
            secret,
            ANALYSIS_RUNNER_SERVICE_ACCOUNT,
            SecretMembership.ACCESSOR,
        )

    def setup_sample_metadata_access_secrets(self):
        """
        sample-metadata-main-read-group-cache-secret
        sample-metadata-main-write-group-cache-secret
        sample-metadata-test-read-group-cache-secret
        sample-metadata-test-write-group-cache-secret
                :return:
        """
        for key, sm_group in self.sample_metadata_groups.items():
            secret = self._setup_group_cache_secret(
                sm_group,
                key=f'sample-metadata-{key}',
                # oops, shouldn't have included the dataset in the original
                # secret name, will be fixed by the new group-cache anyway
                secret_name=f'{self.config.dataset}-sample-metadata-{key}-members-cache',
            )

            self.infra.add_secret_member(
                f'sample-metadata-{key}-api-secret-accessor',
                secret,
                SAMPLE_METADATA_API_SERVICE_ACCOUNT,
                SecretMembership.ACCESSOR,
            )

    def setup_web_access_group_cache(self):
        # Allow list of access-group
        secret = self._setup_group_cache_secret(self.web_access_group, 'web-access')

        self.infra.add_secret_member(
            'web-access-group-cache-secret-accessor',
            secret,
            WEB_SERVER_SERVICE_ACCOUNT,
            SecretMembership.ACCESSOR,
        )

    # endregion ACCESS GROUP CACHE
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
            dataset="fewgenomes",
            deploy_locations=['dev'],
            hail_service_account_test="fewgenomes-test@service-account",
            hail_service_account_standard="fewgenomes-standard@service-account",
            hail_service_account_full="fewgenomes-full@service-account",
        )
        CPGInfrastructure(_infra, _config).main()
