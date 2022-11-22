# pylint: disable=import-error,too-many-public-methods,missing-function-docstring
"""
CPG Dataset infrastructure
"""
import re
from inspect import isclass
from typing import Type, Any, Iterator, Iterable
from collections import defaultdict, namedtuple
from functools import lru_cache, cached_property

import pulumi
import cpg_utils.config

from cpg_infra.abstraction.azure import AzureInfra
from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.abstraction.base import (
    CloudInfraBase,
    DryRunInfra,
    SecretMembership,
    BucketMembership,
    ContainerRegistryMembership,
)
from cpg_infra.config import (
    CPGDatasetConfig,
    CPGDatasetComponents,
    CPGInfrastructureConfig,
)


SampleMetadataAccessorMembership = namedtuple(
    'SampleMetadataAccessorMembership',
    ['name', 'member', 'permissions'],
)

SM_TEST_READ = 'test-read'
SM_TEST_WRITE = 'test-write'
SM_MAIN_READ = 'main-read'
SM_MAIN_WRITE = 'main-write'
SAMPLE_METADATA_PERMISSIONS = [
    SM_TEST_READ,
    SM_TEST_WRITE,
    SM_MAIN_READ,
    SM_MAIN_WRITE,
]


AccessLevel = str
ACCESS_LEVELS: Iterable[AccessLevel] = ('test', 'standard', 'full')
NON_NAME_REGEX = re.compile(r'[^A-Za-z\d_-]')


class CpgDatasetInfrastructure:
    """
    Logic for building infrastructure for a single dataset
    for one infrastructure object.
    """

    @staticmethod
    def deploy_all_from_config(
        config: CPGInfrastructureConfig, dataset_config: CPGDatasetConfig
    ):
        infra_map = {c.name(): c for c in CloudInfraBase.__subclasses__()}

        for infra_obj in [infra_map[n] for n in dataset_config.deploy_locations]:
            CpgDatasetInfrastructure(config, infra_obj, dataset_config).main()

    def __init__(
        self,
        config: CPGInfrastructureConfig,
        infra: CloudInfraBase | Type[CloudInfraBase],
        dataset_config: CPGDatasetConfig,
    ):
        self.config = config
        self.dataset_config: CPGDatasetConfig = dataset_config
        self.infra: CloudInfraBase = (
            infra(self.config, self.dataset_config) if isclass(infra) else infra
        )
        self.components: list[CPGDatasetComponents] = dataset_config.components.get(
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
        self.should_setup_analysis_runner = (
            CPGDatasetComponents.ANALYSIS_RUNNER in self.components
        )

    def create_group(self, name: str):
        group_name = f'{self.dataset_config.dataset}-{name}'
        group = self.infra.create_group(group_name)
        return group

    def main(self):

        # access-groups
        self.setup_access_groups()

        # optional components

        if self.should_setup_storage:
            self.setup_storage()
        if self.should_setup_sample_metadata:
            self.setup_sample_metadata()
        if self.should_setup_hail:
            self.setup_hail()
        if self.should_setup_cromwell:
            self.setup_cromwell()
        if self.should_setup_spark:
            self.setup_spark()
        if self.should_setup_notebooks:
            self.setup_notebooks()
        if self.should_setup_container_registry:
            self.setup_container_registry()
        if self.dataset_config.enable_shared_project:
            self.setup_shared_project()

        if self.should_setup_analysis_runner:
            self.setup_analysis_runner()

        self.setup_reference()

        self.setup_group_cache()

        self.infra.finalise()

    # region MACHINE ACCOUNTS

    @cached_property
    def main_upload_account(self):
        return self.infra.create_machine_account('main-upload')

    @cached_property
    def working_machine_accounts_by_type(
        self,
    ) -> dict[str, list[tuple[AccessLevel, Any]]]:
        machine_accounts: dict[str, list] = defaultdict(list)

        for access_level, account in self.hail_accounts_by_access_level.items():
            machine_accounts['hail'].append((access_level, account))
        for access_level, account in self.deployment_accounts_by_access_level.items():
            machine_accounts['deployment'].append((access_level, account))
        for (
            access_level,
            account,
        ) in self.dataproc_machine_accounts_by_access_level.items():
            machine_accounts['dataproc'].append((access_level, account))
        for (
            access_level,
            account,
        ) in self.cromwell_machine_accounts_by_access_level.items():
            machine_accounts['cromwell'].append((access_level, account))

        return machine_accounts

    def working_machine_accounts_kind_al_account_gen(
        self,
    ) -> Iterator[tuple[str, AccessLevel, Any]]:
        for kind, values in self.working_machine_accounts_by_type.items():
            for access_level, machine_account in values:
                yield kind, access_level, machine_account

    def working_machine_accounts_by_access_level(self):
        machine_accounts: dict[AccessLevel, list[Any]] = defaultdict(list)
        for _, values in self.working_machine_accounts_by_type.items():
            for access_level, machine_account in values:
                machine_accounts[access_level].append(machine_account)

        return machine_accounts

    @cached_property
    def deployment_accounts_by_access_level(self):
        accounts = {
            'test': self.dataset_config.deployment_service_account_test,
            'standard': self.dataset_config.deployment_service_account_standard,
            'full': self.dataset_config.deployment_service_account_full,
        }
        return {k: v for k, v in accounts.items() if v}

    # endregion MACHINE ACCOUNTS
    # region ACCESS GROUPS

    def setup_access_groups(self):
        self.setup_web_access_group_memberships()
        self.setup_access_level_group_memberships()
        self.setup_dependencies_group_memberships()
        self.setup_access_level_group_outputs()

        if isinstance(self.infra, GcpInfrastructure):
            self.setup_gcp_monitoring_access()

    @cached_property
    def access_group(self):
        return self.create_group('access')

    @cached_property
    def web_access_group(self):
        return self.create_group('web-access')

    @cached_property
    def release_access_group(self):
        return self.create_group('release-access')

    @cached_property
    def access_level_groups(self) -> dict[AccessLevel, Any]:
        return {al: self.create_group(al) for al in ACCESS_LEVELS}

    @staticmethod
    def get_group_output_name(*, infra_name: str, dataset: str, kind: str):
        return f'{infra_name}-{dataset}-{kind}-group-id'

    def setup_web_access_group_memberships(self):
        self.infra.add_group_member(
            'web-access-group-access-group-membership',
            group=self.web_access_group,
            member=self.access_group,
        )

    def setup_access_level_group_outputs(self):

        if isinstance(self.infra, DryRunInfra):
            return

        kinds = {
            'access': self.access_group,
            **self.access_level_groups,
        }

        for kind, group in kinds.items():
            pulumi.export(
                self.get_group_output_name(
                    infra_name=self.infra.name(),
                    dataset=self.dataset_config.dataset,
                    kind=kind,
                ),
                group.id if hasattr(group, 'id') else group,
            )

    def setup_access_level_group_memberships(self):
        for (
            kind,
            access_level,
            machine_account,
        ) in self.working_machine_accounts_kind_al_account_gen():
            group = self.access_level_groups[access_level]
            self.infra.add_group_member(
                f'{kind}-{access_level}-access-level-group-membership',
                group=group,
                member=machine_account,
            )

    def setup_gcp_monitoring_access(self):
        assert isinstance(self.infra, GcpInfrastructure)

        self.infra.add_project_role(
            'project-compute-viewer',
            role='roles/compute.viewer',
            member=self.access_group,
            project=self.infra.project_id,
        )

        self.infra.add_project_role(
            'project-logging-viewer',
            role='roles/logging.viewer',
            member=self.access_group,
            project=self.infra.project_id,
        )

        self.infra.add_project_role(
            'project-monitoring-viewer',
            member=self.access_group,
            role='roles/monitoring.viewer',
        )

    # endregion ACCESS GROUPS
    # region STORAGE

    def setup_storage(self):
        if not self.should_setup_storage:
            return

        self.infra.give_member_ability_to_list_buckets(
            'project-buckets-lister', self.access_group
        )
        self.setup_storage_archive_bucket_permissions()
        self.setup_storage_main_bucket_permissions()
        self.setup_storage_main_tmp_bucket()
        self.setup_storage_main_analysis_bucket()
        self.setup_storage_main_web_bucket_permissions()
        self.setup_storage_main_upload_buckets_permissions()
        self.setup_storage_test_buckets_permissions()

        if self.dataset_config.enable_release:
            self.setup_storage_release_bucket_permissions()

        if isinstance(self.infra, GcpInfrastructure):
            self.setup_storage_gcp_requester_pays_access()

    def setup_storage_gcp_requester_pays_access(self):
        """
        Allows the usage of requester-pays buckets for
        access + test + standard + full groups
        :return:
        """
        assert isinstance(self.infra, GcpInfrastructure)

        kinds = {
            'access-group': self.access_group,
            **self.access_level_groups,
        }
        for key, account in kinds.items():
            # Allow the usage of requester-pays buckets.
            self.infra.add_project_role(
                f'{key}-serviceusage-consumer',
                role='roles/serviceusage.serviceUsageConsumer',
                member=account,
            )

    def setup_storage_archive_bucket_permissions(self):
        self.infra.add_member_to_bucket(
            'full-archive-bucket-admin',
            self.archive_bucket,
            self.access_level_groups['full'],
            BucketMembership.MUTATE,
        )

    @cached_property
    def archive_bucket(self):
        return self.infra.create_bucket(
            'archive',
            lifecycle_rules=[
                self.infra.bucket_rule_archive(days=self.dataset_config.archive_age),
                self.infra.bucket_rule_undelete(),
            ],
        )

    # region MAIN BUCKETS

    def setup_storage_main_bucket_permissions(self):
        # access has list permission

        self.infra.add_member_to_bucket(
            'standard-main-bucket-view-create',
            self.main_bucket,
            self.access_level_groups['standard'],
            BucketMembership.APPEND,
        )

        self.infra.add_member_to_bucket(
            'full-main-bucket-admin',
            self.main_bucket,
            self.access_level_groups['full'],
            BucketMembership.MUTATE,
        )

    def setup_storage_main_tmp_bucket(self):
        self.infra.add_member_to_bucket(
            'standard-main-tmp-bucket-view-create',
            self.main_tmp_bucket,
            self.access_level_groups['standard'],
            BucketMembership.APPEND,
        )

        self.infra.add_member_to_bucket(
            'full-main-tmp-bucket-admin',
            self.main_tmp_bucket,
            self.access_level_groups['full'],
            BucketMembership.MUTATE,
        )

    def setup_storage_main_analysis_bucket(self):
        self.infra.add_member_to_bucket(
            'access-group-main-analysis-bucket-viewer',
            self.main_analysis_bucket,
            self.access_group,
            BucketMembership.READ,
        )
        self.infra.add_member_to_bucket(
            'standard-main-analysis-bucket-view-create',
            self.main_analysis_bucket,
            self.access_level_groups['standard'],
            BucketMembership.APPEND,
        )

        self.infra.add_member_to_bucket(
            'full-main-analysis-bucket-admin',
            self.main_analysis_bucket,
            self.access_level_groups['full'],
            BucketMembership.MUTATE,
        )

    def setup_storage_main_web_bucket_permissions(self):
        self.infra.add_member_to_bucket(
            'access-group-main-web-bucket-viewer',
            self.main_web_bucket,
            self.access_group,
            BucketMembership.READ,
        )

        # web-server
        if not isinstance(self.infra, AzureInfra):
            self.infra.add_member_to_bucket(
                'web-server-main-web-bucket-viewer',
                self.main_web_bucket,
                self.config.web_service.gcp.server_machine_account,  # WEB_SERVER_SERVICE_ACCOUNT,
                BucketMembership.READ,
            )

        self.infra.add_member_to_bucket(
            'standard-main-web-bucket-view-create',
            self.main_web_bucket,
            self.access_level_groups['standard'],
            BucketMembership.APPEND,
        )

        self.infra.add_member_to_bucket(
            'full-main-web-bucket-admin',
            self.main_web_bucket,
            self.access_level_groups['full'],
            BucketMembership.MUTATE,
        )

    def setup_storage_main_upload_buckets_permissions(self):
        for bname, main_upload_bucket in self.main_upload_buckets.items():

            # main_upload SA has ADMIN
            self.infra.add_member_to_bucket(
                f'main-upload-service-account-{bname}-bucket-creator',
                bucket=main_upload_bucket,
                member=self.main_upload_account,
                membership=BucketMembership.MUTATE,
            )

            # full GROUP has ADMIN
            self.infra.add_member_to_bucket(
                f'full-{bname}-bucket-admin',
                bucket=main_upload_bucket,
                member=self.access_level_groups['full'],
                membership=BucketMembership.MUTATE,
            )

            # standard GROUP has READ
            self.infra.add_member_to_bucket(
                f'standard-{bname}-bucket-viewer',
                bucket=main_upload_bucket,
                member=self.access_level_groups['standard'],
                membership=BucketMembership.READ,
            )

            # access GROUP has VIEWER
            # (semi surprising tbh, but useful for reading uploaded metadata)
            self.infra.add_member_to_bucket(
                f'access-group-{bname}-bucket-viewer',
                bucket=main_upload_bucket,
                member=self.access_group,
                membership=BucketMembership.READ,
            )

    @cached_property
    def main_bucket(self):
        return self.infra.create_bucket(
            'main', lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @cached_property
    def main_tmp_bucket(self):
        return self.infra.create_bucket(
            'main-tmp',
            lifecycle_rules=[self.infra.bucket_rule_temporary()],
            versioning=False,
        )

    @cached_property
    def main_analysis_bucket(self):
        return self.infra.create_bucket(
            'main-analysis', lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @cached_property
    def main_web_bucket(self):
        return self.infra.create_bucket(
            'main-web', lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @cached_property
    def main_upload_buckets(self) -> dict[str, Any]:
        main_upload_undelete = self.infra.bucket_rule_undelete(days=30)
        main_upload_buckets = {
            'main-upload': self.infra.create_bucket(
                'main-upload', lifecycle_rules=[main_upload_undelete]
            )
        }

        for additional_upload_bucket in self.dataset_config.additional_upload_buckets:
            main_upload_buckets[additional_upload_bucket] = self.infra.create_bucket(
                additional_upload_bucket,
                lifecycle_rules=[main_upload_undelete],
                unique=True,
            )

        return main_upload_buckets

    # endregion MAIN BUCKETS
    # region TEST BUCKETS

    def setup_storage_test_buckets_permissions(self):
        """
        Test bucket permissions are much more uniform,
        so just work out some more generic mechanism
        """

        buckets = [
            ('test', self.test_bucket),
            ('test-analysis', self.test_analysis_bucket),
            ('test-tmp', self.test_tmp_bucket),
            ('test-web', self.test_web_bucket),
            ('test-upload', self.test_upload_bucket),
        ]

        for bucket_name, bucket in buckets:

            test_bucket_admins: list[tuple[str, Any]] = [
                (f'access-group-{bucket_name}-bucket-admin', self.access_group),
                *[
                    (f'{access_level}-{bucket_name}-bucket-admin', group)
                    for access_level, group in self.access_level_groups.items()
                ],
            ]
            for resource_key, group in test_bucket_admins:
                self.infra.add_member_to_bucket(
                    resource_key,
                    bucket,
                    group,
                    BucketMembership.MUTATE,
                )

        # give web-server access to test-bucket
        if not isinstance(self.infra, AzureInfra):
            self.infra.add_member_to_bucket(
                'web-server-test-web-bucket-viewer',
                bucket=self.test_web_bucket,
                member=self.config.web_service.gcp.server_machine_account,  # WEB_SERVER_SERVICE_ACCOUNT,
                membership=BucketMembership.READ,
            )

    @cached_property
    def test_bucket(self):
        return self.infra.create_bucket(
            'test', lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @cached_property
    def test_analysis_bucket(self):
        return self.infra.create_bucket(
            'test-analysis', lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @cached_property
    def test_web_bucket(self):
        return self.infra.create_bucket(
            'test-web', lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    @cached_property
    def test_tmp_bucket(self):
        return self.infra.create_bucket(
            'test-tmp',
            lifecycle_rules=[self.infra.bucket_rule_temporary()],
            versioning=False,
        )

    @cached_property
    def test_upload_bucket(self):
        return self.infra.create_bucket(
            'test-upload', lifecycle_rules=[self.infra.bucket_rule_undelete()]
        )

    # endregion TEST BUCKETS
    # region RELEASE BUCKETS

    def setup_storage_release_bucket_permissions(self):
        self.infra.add_member_to_bucket(
            'access-group-release-bucket-viewer',
            self.release_bucket,
            self.access_group,
            BucketMembership.READ,
        )

        self.infra.add_member_to_bucket(
            'release-access-group-release-bucket-viewer',
            self.release_bucket,
            self.release_access_group,
            BucketMembership.READ,
        )

        self.infra.add_member_to_bucket(
            'full-release-bucket-admin',
            self.release_bucket,
            self.access_level_groups['full'],
            BucketMembership.MUTATE,
        )

    @cached_property
    def release_bucket(self):
        return self.infra.create_bucket(
            'release',
            lifecycle_rules=[self.infra.bucket_rule_undelete()],
            requester_pays=True,
        )

    # endregion RELEASE BUCKETS
    # endregion STORAGE
    # region HAIL

    def setup_hail(self):
        self.setup_hail_bucket_permissions()
        self.setup_hail_wheels_bucket_permissions()

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
                BucketMembership.MUTATE,
            )

        if self.should_setup_analysis_runner and isinstance(
            self.infra, GcpInfrastructure
        ):
            # TODO: this will be more complicated for Azure, because analysis-runner
            #   needs access to Azure bucket to write wheels / jars
            # The analysis-runner needs Hail bucket access for compiled code.
            self.infra.add_member_to_bucket(
                'analysis-runner-hail-bucket-admin',
                bucket=self.hail_bucket,
                member=self.config.analysis_runner.gcp.server_machine_account,  # ANALYSIS_RUNNER_SERVICE_ACCOUNT,
                membership=BucketMembership.MUTATE,
            )

    def setup_hail_wheels_bucket_permissions(self):
        keys = {'access-group': self.access_group, **self.access_level_groups}

        bucket = None
        if isinstance(self.infra, GcpInfrastructure):
            bucket = self.config.hail.gcp.wheel_bucket_name

        if not bucket:
            return

        for key, group in keys.items():
            self.infra.add_member_to_bucket(
                f'{key}-hail-wheels-viewer',
                bucket=bucket,
                member=group,
                membership=BucketMembership.READ,
            )

    @cached_property
    def hail_accounts_by_access_level(self):
        if not self.should_setup_hail:
            return {}

        accounts = {}
        if isinstance(self.infra, GcpInfrastructure):
            accounts = {
                'test': self.dataset_config.gcp_hail_service_account_test,
                'standard': self.dataset_config.gcp_hail_service_account_standard,
                'full': self.dataset_config.gcp_hail_service_account_full,
            }
        elif isinstance(self.infra, AzureInfra):
            accounts = {
                'test': self.dataset_config.azure_hail_service_account_test,
                'standard': self.dataset_config.azure_hail_service_account_standard,
                'full': self.dataset_config.azure_hail_service_account_full,
            }
        else:
            return accounts

        accounts = {cat: ac for cat, ac in accounts.items() if ac}
        return accounts

    @cached_property
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
        self.setup_cromwell_credentials()

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
                self.config.cromwell.gcp.runner_machine_account,  # CROMWELL_RUNNER_ACCOUNT,
            )

        if isinstance(self.infra, GcpInfrastructure):
            self._gcp_setup_cromwell()

    def setup_cromwell_credentials(self):
        for (
            access_level,
            cromwell_account,
        ) in self.cromwell_machine_accounts_by_access_level.items():
            secret = self.infra.create_secret(
                f'{self.dataset_config.dataset}-cromwell-{access_level}-key',
                project=self.config.analysis_runner.gcp.project,  # ANALYSIS_RUNNER_PROJECT,
            )

            credentials = self.infra.get_credentials_for_machine_account(
                f'cromwell-service-account-{access_level}-key', cromwell_account
            )

            # add credentials to the secret
            self.infra.add_secret_version(
                f'cromwell-service-account-{access_level}-secret-version',
                secret=secret,
                contents=credentials,
            )

            # allow the analysis-runner to view the secret
            self.infra.add_secret_member(
                f'cromwell-service-account-{access_level}-secret-accessor',
                secret=secret,
                member=self.config.analysis_runner.gcp.server_machine_account,  # ANALYSIS_RUNNER_SERVICE_ACCOUNT,
                membership=SecretMembership.ACCESSOR,
                project=self.config.analysis_runner.gcp.project,  # ANALYSIS_RUNNER_PROJECT,
            )

            # Allow the Hail service account to access its corresponding cromwell key
            if self.should_setup_hail:
                hail_service_account = self.hail_accounts_by_access_level[access_level]
                self.infra.add_secret_member(
                    f'cromwell-service-account-{access_level}-self-accessor',
                    project=self.config.analysis_runner.gcp.project,  # ANALYSIS_RUNNER_PROJECT,
                    secret=secret,
                    member=hail_service_account,
                    membership=SecretMembership.ACCESSOR,
                )

    @cached_property
    def cromwell_machine_accounts_by_access_level(self) -> dict[AccessLevel, Any]:
        if not self.should_setup_cromwell:
            return {}

        accounts = {
            access_level: self.infra.create_machine_account(f'cromwell-{access_level}')
            for access_level in ACCESS_LEVELS
        }
        return accounts

    def _gcp_setup_cromwell(self):
        assert isinstance(self.infra, GcpInfrastructure)

        # Add Hail service accounts to (premade) Cromwell access group.
        for access_level, hail_account in self.hail_accounts_by_access_level.items():
            self.infra.add_group_member(
                f'hail-service-account-{access_level}-cromwell-access',
                group=self.config.cromwell.gcp.access_group_id,  # CROMWELL_ACCESS_GROUP_ID,
                member=hail_account,
            )

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
            # Allow the hail account to run jobs AS the spark user
            self.infra.add_member_to_machine_account_access(
                f'hail-service-account-{access_level}-dataproc-service-account-user',
                spark_accounts[access_level],
                hail_account,
            )

        if isinstance(self.infra, GcpInfrastructure):
            for access_level, spark_account in spark_accounts.items():
                # allow the spark_account to run jobs
                self.infra.add_member_to_dataproc_api(
                    f'dataproc-service-account-{access_level}-dataproc-worker',
                    spark_account,
                    f'{self.infra.organization.id}/roles/DataprocWorkerWithoutStorageAccess',
                )

            for (
                access_level,
                hail_account,
            ) in self.hail_accounts_by_access_level.items():

                # Allow hail account to create a cluster
                self.infra.add_member_to_dataproc_api(
                    f'hail-service-account-{access_level}-dataproc-admin',
                    account=hail_account,
                    role='admin',
                )

                # Give hail worker permissions to submit jobs.
                self.infra.add_member_to_dataproc_api(
                    f'hail-service-account-{access_level}-dataproc-worker',
                    account=hail_account,
                    role=f'{self.infra.organization.id}/roles/DataprocWorkerWithoutStorageAccess',
                )

            self.infra.add_project_role(
                'project-dataproc-viewer',
                role='roles/dataproc.viewer',
                member=self.access_group,
                project=self.infra.project_id,
            )

    @cached_property
    def dataproc_machine_accounts_by_access_level(self) -> dict[AccessLevel, Any]:
        if not self.should_setup_spark:
            return {}

        accounts = {
            access_level: self.infra.create_machine_account(f'dataproc-{access_level}')
            for access_level in ACCESS_LEVELS
        }
        return accounts

    # endregion SPARK
    # region SAMPLE METADATA

    def setup_sample_metadata(self):
        if not self.should_setup_sample_metadata:
            return

        self.setup_sample_metadata_access_permissions()

        if isinstance(self.infra, GcpInfrastructure):
            # do some cloudrun stuff
            self.setup_sample_metadata_cloudrun_permissions()
        elif isinstance(self.infra, AzureInfra):
            # we'll do some custom stuff here :)
            raise NotImplementedError

    @cached_property
    def sample_metadata_groups(self) -> dict[str, Any]:
        if not self.should_setup_sample_metadata:
            return {}

        sm_groups = {
            key: self.create_group(f'sample-metadata-{key}')
            for key in SAMPLE_METADATA_PERMISSIONS
        }

        return sm_groups

    def setup_sample_metadata_cloudrun_permissions(self):
        # now we give the sample_metadata_access_group access to cloud-run instance
        assert isinstance(self.infra, GcpInfrastructure)

        for sm_type, group in self.sample_metadata_groups.items():
            self.infra.add_cloudrun_invoker(
                f'sample-metadata-{sm_type}-cloudrun-invoker',
                service=self.config.sample_metadata.gcp.service_name,  # SAMPLE_METADATA_SERVICE_NAME,
                project=self.config.sample_metadata.gcp.project,  # SAMPLE_METADATA_PROJECT,
                member=group,
            )

        self.infra.add_cloudrun_invoker(
            f'sample-metadata-access-group-cloudrun-invoker',
            service=self.config.sample_metadata.gcp.service_name,  # SAMPLE_METADATA_SERVICE_NAME,
            project=self.config.sample_metadata.gcp.project,  # SAMPLE_METADATA_PROJECT,
            member=self.access_group,
        )

    def setup_sample_metadata_access_permissions(self):
        if not self.should_setup_sample_metadata:
            return
        sm_access_levels: list[SampleMetadataAccessorMembership] = [
            SampleMetadataAccessorMembership(
                name='human',
                member=self.access_group,
                permissions=(SM_MAIN_READ, SM_TEST_READ, SM_TEST_WRITE),
            ),
            SampleMetadataAccessorMembership(
                name='test',
                member=self.access_level_groups['test'],
                permissions=(SM_MAIN_READ, SM_TEST_READ, SM_TEST_WRITE),
            ),
            SampleMetadataAccessorMembership(
                name='standard',
                member=self.access_level_groups['standard'],
                permissions=(SM_MAIN_READ, SM_MAIN_WRITE),
            ),
            SampleMetadataAccessorMembership(
                name='full',
                member=self.access_level_groups['full'],
                permissions=SAMPLE_METADATA_PERMISSIONS,
            ),
            # allow the analysis-runner logging cloud function to update the sample-metadata project
            SampleMetadataAccessorMembership(
                name='analysis-runner-logger',
                member=self.config.analysis_runner.gcp.logger_machine_account,
                permissions=SAMPLE_METADATA_PERMISSIONS,
            ),
        ]

        # extra custom SAs
        extra_sm_read_sas = self.dataset_config.sm_read_only_sas
        extra_sm_write_sas = self.dataset_config.sm_read_write_sas

        for sa in extra_sm_read_sas:
            sm_access_levels.append(
                SampleMetadataAccessorMembership(
                    name=self._get_name_from_external_sa(sa),
                    member=sa,
                    permissions=(SM_MAIN_READ,),
                )
            )
        for sa in extra_sm_write_sas:
            sm_access_levels.append(
                SampleMetadataAccessorMembership(
                    name=self._get_name_from_external_sa(sa),
                    member=sa,
                    permissions=(SM_MAIN_READ, SM_MAIN_WRITE),
                )
            )

        for name, member, permission in sm_access_levels:
            for kind in permission:
                self.infra.add_group_member(
                    f'sample-metadata-{kind}-{name}-access-level-group-membership',
                    group=self.sample_metadata_groups[kind],
                    member=member,
                )

    # endregion SAMPLE METADATA
    # region CONTAINER REGISTRY

    def setup_container_registry(self):
        """
        Give compute-accounts access to analysis-runner
        + cpg-common container registries
        :return:
        """
        self.setup_dataset_container_registry()
        self.setup_legacy_container_registries()

    def setup_dataset_container_registry(self):
        """
        If required, setup a container registry for a dataset
        :return:
        """
        if not self.dataset_config.create_container_registry:
            return

        # mostly because this current format requires the project_id
        custom_container_registry = self.infra.create_container_registry('images')
        for kind, account in self.access_level_groups.items():
            self.infra.add_member_to_container_registry(
                f'{kind}-images-reader-in-container-registry',
                registry=custom_container_registry,
                member=account,
                membership=ContainerRegistryMembership.READER,
            )
            if kind in ('standard', 'full'):
                self.infra.add_member_to_container_registry(
                    f'{kind}-images-writer-in-container-registry',
                    registry=custom_container_registry,
                    member=account,
                    membership=ContainerRegistryMembership.WRITER,
                )

    def setup_legacy_container_registries(self):
        """
        Setup permissions for analysis-runner artifact registries
        """
        # TODO: This will eventually be mostly solved by the cpg-common
        #       dataset with permissions through inheritance.
        if not isinstance(self.infra, GcpInfrastructure):
            return
        try:
            if not self.config.analysis_runner.gcp.project:
                return
        except AttributeError:
            # gross catch nulls
            return

        container_registries = [
            (
                self.config.analysis_runner.gcp.project,
                self.config.analysis_runner.gcp.container_registry_name,
            ),
        ]

        kinds = {
            'access-group': self.access_group,
            **self.access_level_groups,
        }

        for kind, account in kinds.items():

            # Allow the service accounts to pull images. Note that the global project will
            # refer to the dataset, but the Docker images are stored in the 'analysis-runner'
            # and 'cpg-common' projects' Artifact Registry repositories.
            for project, registry_name in container_registries:
                self.infra.add_member_to_container_registry(
                    f'{kind}-images-reader-in-{project}',
                    registry=registry_name,
                    project=project,
                    member=account,
                    membership=ContainerRegistryMembership.READER,
                )

    # endregion CONTAINER REGISTRY
    # region NOTEBOOKS

    def setup_notebooks(self):
        self.setup_notebooks_account_permissions()

    def setup_notebooks_account_permissions(self):

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

        if isinstance(self.infra, GcpInfrastructure):
            self.infra.add_project_role(
                'notebook-account-compute-admin',
                project=self.config.notebooks.gcp.project,  # NOTEBOOKS_PROJECT,
                role='roles/compute.admin',
                member=self.notebook_account,
            )
        elif isinstance(self.infra, DryRunInfra):
            pass
        else:
            # TODO: How to abstract compute.admin on project
            raise NotImplementedError(
                f'No implementation for compute.admin for notebook account on {self.infra.name()}'
            )

    @cached_property
    def notebook_account(self):
        return self.infra.create_machine_account(
            f'notebook-{self.dataset_config.dataset}',
            project=self.config.notebooks.gcp.project,
        )

    # endregion NOTEBOOKS
    # region ANALYSIS RUNNER

    def setup_analysis_runner(self):

        self.setup_analysis_runner_config_access()

        if isinstance(self.infra, GcpInfrastructure):
            self.setup_analysis_runner_access()

    def setup_analysis_runner_access(self):

        assert isinstance(self.infra, GcpInfrastructure)
        self.infra.add_cloudrun_invoker(
            f'analysis-runner-access-invoker',
            project=self.config.analysis_runner.gcp.project,  # ANALYSIS_RUNNER_PROJECT,
            service=self.config.analysis_runner.gcp.cloud_run_instance_name,  # ANALYSIS_RUNNER_CLOUD_RUN_INSTANCE_NAME,
            member=self.access_group,
        )

    def setup_analysis_runner_config_access(self):
        keys = {'access-group': self.access_group, **self.access_level_groups}

        for key, group in keys.items():
            self.infra.add_member_to_bucket(
                f'{key}-analysis-runner-config-viewer',
                bucket=self.config.gcp.config_bucket_name,  # ANALYSIS_RUNNER_CONFIG_BUCKET_NAME,
                member=group,
                membership=BucketMembership.READ,
            )

    # endregion ANALYSIS RUNNER

    # region SHARED PROJECT

    def setup_shared_project(self):
        if not self.dataset_config.enable_shared_project:
            return

        if not self.dataset_config.enable_release:
            raise ValueError(
                'Requested shared project, but no bucket is available to share.'
            )

        if not self.dataset_config.shared_project_budget:
            raise ValueError(
                'Requested shared project, but the dataset configuration option '
                '"shared_project_budget" was not specified.'
            )

        shared_buckets = {'release': self.release_bucket}

        project_name = f'{self.infra.get_dataset_project_id()}-shared'

        shared_project = self.infra.create_project(project_name)
        self.infra.create_fixed_budget(
            f'{self.dataset_config.dataset}-shared-budget',
            project=shared_project,
            budget=self.dataset_config.shared_project_budget,
        )

        shared_ma = self.infra.create_machine_account(
            'shared',
            project=shared_project,
            resource_key='budget-shared-service-account',
        )

        if isinstance(self.infra, GcpInfrastructure):
            self.infra.add_project_role(
                # Allow the usage of requester-pays buckets.
                'shared-project-serviceusage-consumer',
                role='roles/serviceusage.serviceUsageConsumer',
                member=shared_ma,
                project=shared_project,
            )

        for bname, bucket in shared_buckets.items():
            self.infra.add_member_to_bucket(
                f'{bname}-shared-membership',
                bucket=bucket,
                member=shared_ma,
                membership=BucketMembership.READ,
            )

    # endregion SHARED PROJECT

    # region ACCESS GROUP CACHE

    def setup_group_cache(self):
        if not isinstance(self.infra, GcpInfrastructure):
            return

        self.setup_group_cache_access_group()
        self.setup_group_cache_web_access_group()
        self.setup_group_cache_sample_metadata_secrets()

    def _setup_group_cache_secret(self, *, group, key, secret_name: str = None):
        self.infra.add_group_member(
            f'group-cache-{key}-membership',
            group,
            self.config.access_group_cache.process_machine_account,  # ACCESS_GROUP_CACHE_SERVICE_ACCOUNT,
        )
        group_cache_secret = self.infra.create_secret(
            secret_name or f'{key}-group-cache-secret',
        )
        # Modify access_group_cache secret
        self.infra.add_secret_member(
            f'{key}-group-cache-secret-version-manager',
            group_cache_secret,
            self.config.access_group_cache.process_machine_account,  # ACCESS_GROUP_CACHE_SERVICE_ACCOUNT,
            SecretMembership.ADMIN,
        )

        self.infra.add_secret_member(
            f'{key}-group-cache-secret-accessor',
            group_cache_secret,
            self.config.access_group_cache.process_machine_account,  # ACCESS_GROUP_CACHE_SERVICE_ACCOUNT,
            SecretMembership.ACCESSOR,
        )

        return group_cache_secret

    def setup_group_cache_access_group(self):
        # Allow list of access-group

        for key, group in self.access_level_groups.items():
            # setup secret
            _ = self._setup_group_cache_secret(
                group=group,
                key=key,
                secret_name=f'{self.dataset_config.dataset}-{key}-members-cache',
            )

        # analysis-runner view contents of access-groups

        access_secret = self._setup_group_cache_secret(
            group=self.access_group,
            key='access',
            secret_name=f'{self.dataset_config.dataset}-access-members-cache',
        )
        self.infra.add_secret_member(
            f'analysis-runner-access-group-cache-secret-accessor',
            access_secret,
            self.config.analysis_runner.gcp.server_machine_account,  # ANALYSIS_RUNNER_SERVICE_ACCOUNT,
            SecretMembership.ACCESSOR,
        )

    def setup_group_cache_sample_metadata_secrets(self):
        """
        sample-metadata-main-read-group-cache-secret
        sample-metadata-main-write-group-cache-secret
        sample-metadata-test-read-group-cache-secret
        sample-metadata-test-write-group-cache-secret
                :return:
        """
        for key, sm_group in self.sample_metadata_groups.items():
            secret = self._setup_group_cache_secret(
                group=sm_group,
                key=f'sample-metadata-{key}',
                # oops, shouldn't have included the dataset in the original
                # secret name, will be fixed by the new group-cache anyway
                secret_name=f'{self.dataset_config.dataset}-sample-metadata-{key}-members-cache',
            )

            self.infra.add_secret_member(
                f'sample-metadata-{key}-api-secret-accessor',
                secret,
                self.config.sample_metadata.gcp.machine_account,  # SAMPLE_METADATA_API_SERVICE_ACCOUNT,
                SecretMembership.ACCESSOR,
            )

    def setup_group_cache_web_access_group(self):
        # Allow list of access-group
        secret = self._setup_group_cache_secret(
            group=self.web_access_group,
            key='web-access',
            secret_name=f'{self.dataset_config.dataset}-web-access-members-cache',
        )

        self.infra.add_secret_member(
            'web-server-web-access-group-cache-secret-accessor',
            secret,
            self.config.web_service.gcp.server_machine_account,  # WEB_SERVER_SERVICE_ACCOUNT,
            SecretMembership.ACCESSOR,
        )

    # endregion ACCESS GROUP CACHE
    # region REFERENCE

    def setup_reference(self):

        kinds = {
            'access-group': self.access_group,
            **self.access_level_groups,
        }

        if isinstance(self.infra, GcpInfrastructure):
            for kind, group in kinds.items():
                self.infra.add_member_to_bucket(
                    f'{kind}-reference-bucket-viewer',
                    bucket=self.config.gcp.reference_bucket_name,  # REFERENCE_BUCKET_NAME,
                    member=group,
                    membership=BucketMembership.READ,
                )

    # endregion REFERENCE
    # region DEPENDENCIES

    def setup_dependencies(self):
        self.setup_dependencies_group_memberships()

    def setup_dependencies_group_memberships(self):

        # duplicate reference to avoid mutating config
        dependencies = list(self.dataset_config.depends_on)

        if self.dataset_config.dataset != self.config.reference_dataset:
            dependencies.append(self.config.reference_dataset)

        for dependency in dependencies:
            dependent_stack = self.get_pulumi_stack(dependency)

            self.infra.add_group_member(
                resource_key=f'{dependency}-access-group',
                group=dependent_stack.get_output(
                    self.get_group_output_name(
                        infra_name=self.infra.name(), dataset=dependency, kind='access'
                    )
                ),
                member=self.access_group,
            )

            for access_level, primary_access_group in self.access_level_groups.items():
                dependency_group_id = dependent_stack.get_output(
                    self.get_group_output_name(
                        infra_name=self.infra.name(),
                        dataset=dependency,
                        kind=access_level,
                    ),
                )

                # add this dataset to dependencies membership
                self.infra.add_group_member(
                    f'{dependency}-{access_level}-access-level-group',
                    dependency_group_id,
                    primary_access_group,
                )

    # endregion DEPENDENCIES
    # region UTILS

    @staticmethod
    @lru_cache()
    def get_pulumi_stack(dependency_name: str):
        return pulumi.StackReference(dependency_name)

    @staticmethod
    def _get_name_from_external_sa(email: str, suffix='.iam.gserviceaccount.com'):
        """
        Convert service account email to name + some filtering.

        >>> CpgDatasetInfrastructure._get_name_from_external_sa('my-service-account@project.iam.gserviceaccount.com')
        'my-service-account-project'

        >>> CpgDatasetInfrastructure._get_name_from_external_sa('yourname@populationgenomics.org.au')
        'yourname'

        >>> CpgDatasetInfrastructure._get_name_from_external_sa('my.service-account+extra@domain.com')
        'my-service-account-extra'
        """
        if email.endswith(suffix):
            base = email[: -len(suffix)]
        else:
            base = email.split('@')[0]

        return NON_NAME_REGEX.sub('-', base).replace('--', '-')

    # endregion UTILS


if __name__ == '__main__':

    locations: list[Type[CloudInfraBase]] = [
        DryRunInfra,
        # GcpInfrastructure,
        # AzureInfra,
    ]
    infra_config = CPGInfrastructureConfig.from_dict(cpg_utils.config.get_config())

    for location in locations:

        _config = CPGDatasetConfig(
            dataset='fewgenomes',
            deploy_locations=['dry-run'],
            gcp_hail_service_account_test='fewgenomes-test@service-account',
            gcp_hail_service_account_standard='fewgenomes-standard@service-account',
            gcp_hail_service_account_full='fewgenomes-full@service-account',
        )
        CpgDatasetInfrastructure(infra_config, location, _config).main()
