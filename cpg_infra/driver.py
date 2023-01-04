# pylint: disable=import-error,too-many-public-methods,missing-function-docstring
"""
CPG Dataset infrastructure
"""
import re
import os.path
import graphlib
from typing import Type, Any, Iterator, Iterable
from collections import defaultdict, namedtuple
from functools import cached_property

import toml
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
TOML_CONFIG_JOINER = '\n||||'


class CPGInfrastructure:
    """Class for managing all CPG infrastructure"""

    class GroupProvider:
        """Provider for managing groups + memberships"""

        class Group:
            """Placeholder for a Group of members"""

            def __init__(self, name: str, members: dict, cache_members: bool):
                self.name: str = name
                self.cache_members: bool = cache_members
                self.members: dict[str, Any] = members

            def add_member(self, resource_key, member):
                print(f'{resource_key} :: {self.name}.add_member({member})')
                self.members[resource_key] = member

            def __repr__(self):
                return f'GROUP("{self.name}")'

        def __init__(self):
            self.groups: dict[
                str, dict[str, CPGInfrastructure.GroupProvider.Group]
            ] = defaultdict()
            self._cached_resolved_members: dict[str, list] = {}

        def get_group(self, infra_name: str, group_name: str):
            return self.groups[infra_name][group_name]

        def create_group(
            self,
            infra: CloudInfraBase,
            name: str,
            cache_members: bool,
            members: dict = None,
        ) -> Group:
            if infra.name() not in self.groups:
                self.groups[infra.name()] = {}
            if name in self.groups[infra.name()]:
                raise ValueError(f'Group "{name}" in "{infra.name()}" already exists')

            group = CPGInfrastructure.GroupProvider.Group(
                name=name,
                cache_members=cache_members,
                members=members or {},
            )
            self.groups[infra.name()][name] = group

            return group

        def static_group_order(self, cloud) -> list[Group]:
            """
            not that it super matters because we do recursively look it up,
            but it's nice to grab the groups in an order that minimises depth looking
            """
            groups = self.groups[cloud]

            deps = {
                group.name: [
                    g.name
                    for g in group.members.values()
                    if isinstance(g, CPGInfrastructure.GroupProvider.Group)
                ]
                for group in groups.values()
            }

            return [groups[n] for n in graphlib.TopologicalSorter(deps).static_order()]

        def resolve_group_members(self, group: 'Group') -> list:
            if group.name in self._cached_resolved_members:
                return self._cached_resolved_members[group.name]

            resolved_members = []
            for member in group.members.values():
                if isinstance(member, CPGInfrastructure.GroupProvider.Group):
                    resolved_members.extend(self.resolve_group_members(member))
                else:
                    resolved_members.append(member)

            self._cached_resolved_members[group.name] = resolved_members
            return resolved_members

    def __init__(
        self, config: CPGInfrastructureConfig, dataset_configs: list[CPGDatasetConfig]
    ):
        self.config = config
        self.datasets = {d.dataset: d for d in dataset_configs}
        self.group_provider = CPGInfrastructure.GroupProvider()

        # { cloud: { name: DatasetInfrastructure } }
        self.dataset_infrastructure: dict[
            str, dict[str, CPGDatasetInfrastructure]
        ] = defaultdict()

    def resolve_dataset_order(self):
        reference_dataset = (
            [self.config.reference_dataset] if self.config.reference_dataset else []
        )
        deps = {k: v.depends_on + reference_dataset for k, v in self.datasets.items()}
        if self.config.reference_dataset:
            deps[self.config.reference_dataset] = []

        return graphlib.TopologicalSorter(deps).static_order()

    def main(self):
        self.setup_access_cache_bucket()
        self.deploy_datasets()
        self.finalize_groups()

    def deploy_datasets(self):
        infra_map: dict[str, Type[CloudInfraBase]] = {
            c.name(): c for c in CloudInfraBase.__subclasses__()
        }

        for dataset in self.resolve_dataset_order():
            dataset_config = self.datasets[dataset]

            for deploy_location in dataset_config.deploy_locations:

                location = infra_map[deploy_location]
                infra_obj = location(
                    config=self.config,
                    dataset_config=dataset_config,
                )
                dataset_infra = CPGDatasetInfrastructure(
                    root=self,
                    config=self.config,
                    infra=infra_obj,
                    dataset_config=dataset_config,
                    group_provider=self.group_provider,
                )
                dataset_infra.main()
                self.dataset_infrastructure[deploy_location][dataset] = dataset_infra

    def finalize_groups(self):
        infra_map: dict[str, Type[CloudInfraBase]] = {
            c.name(): c for c in CloudInfraBase.__subclasses__()
        }
        # now resolve groups
        for cloud in self.group_provider.groups:
            infra = infra_map[cloud](config=self.config, dataset_config=None)

            for group in self.group_provider.static_group_order(cloud=cloud):
                igroup = infra.create_group(group.name)

                for resource_key, member in group.members.items():
                    infra.add_group_member(
                        resource_key=resource_key, group=igroup, member=member
                    )

                if group.cache_members:
                    _members = self.group_provider.resolve_group_members(group)
                    member_ids = [infra.member_id(m) for m in _members]
                    if all(isinstance(m, str) for m in member_ids):
                        members_contents = '\n'.join(member_ids)
                    else:
                        members_contents = pulumi.Output.all(*member_ids).apply(
                            '\n'.join
                        )

                    # we'll create a blob with the members of the groups
                    infra.add_blob_to_bucket(
                        f'{group.name}-group-cache-members',
                        bucket=self.access_cache_bucket,
                        contents=members_contents,
                        output_name=f'{group.name}-members.txt',
                    )

    # dataset agnostic infrastructure

    # region ACCESS_CACHE

    @cached_property
    def access_cache_bucket(self):
        reference_infra = self.dataset_infrastructure['gcp'][
            self.config.reference_dataset
        ]
        return reference_infra.infra.create_bucket(
            'cpg-access-group-cache', unique=True, versioning=True, lifecycle_rules=[]
        )

    def setup_access_cache_bucket(self):
        reference_infra = self.dataset_infrastructure['gcp'][
            self.config.reference_dataset
        ]

        access_group_cache_accessors = []

        if self.config.analysis_runner:
            access_group_cache_accessors.append(
                (
                    'analysis-runner',
                    self.config.analysis_runner.gcp.server_machine_account,
                )
            )

        if self.config.sample_metadata:
            access_group_cache_accessors.append(
                ('sample-metadata', self.config.sample_metadata.gcp.machine_account)
            )

        if self.config.web_service:
            access_group_cache_accessors.append(
                ('web-service', self.config.web_service.gcp.server_machine_account)
            )

        for key, account in access_group_cache_accessors:

            reference_infra.infra.add_member_to_bucket(
                f'{key}-access-group-cache-accessor',
                bucket=self.access_cache_bucket,
                member=account,
                membership=BucketMembership.READ,
            )


class CPGDatasetInfrastructure:
    """
    Logic for building infrastructure for a single dataset
    for one infrastructure object.
    """

    def __init__(
        self,
        config: CPGInfrastructureConfig,
        root: CPGInfrastructure,
        group_provider: CPGInfrastructure.GroupProvider,
        infra: CloudInfraBase,
        dataset_config: CPGDatasetConfig,
    ):
        self.config = config
        self.root = root
        self.group_provider = group_provider

        self.dataset_config: CPGDatasetConfig = dataset_config
        self.infra: CloudInfraBase = infra
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

        # outputs
        self.storage_tomls = {}

    def create_group(self, name: str, cache_members: bool = False):
        group_name = f'{self.dataset_config.dataset}-{name}'
        # group = self.infra.create_group(group_name)
        group = self.group_provider.create_group(
            self.infra, cache_members=cache_members, name=group_name
        )
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

        # transitive person groups
        self.metadata_access_group.add_member(
            self.infra.get_pulumi_name('data-manager-in-metadata'),
            self.data_manager_group,
        )
        self.metadata_access_group.add_member(
            self.infra.get_pulumi_name('analysis-in-metadata'), self.analysis_group
        )
        self.metadata_access_group.add_member(
            self.infra.get_pulumi_name('metadata-in-metadata-web'),
            self.web_access_group,
        )

        if isinstance(self.infra, GcpInfrastructure):
            self.setup_gcp_monitoring_access()

    @cached_property
    def data_manager_group(self):
        return self.create_group('data-manager')

    @cached_property
    def analysis_group(self):
        return self.create_group('access', cache_members=True)

    @cached_property
    def metadata_access_group(self):
        return self.create_group('metadata-access')

    @cached_property
    def web_access_group(self):
        return self.create_group('web-access')

    @cached_property
    def upload_group(self):
        """
        We want people to upload machine accounts, so it makes sense for us to
        give collaborators ONE set of credentials, and add those credentials to
        multiple upload groups. This makes who has access to datasets
        more transparent.
        """
        return self.create_group('upload')

    @cached_property
    def release_access_group(self):
        return self.create_group('release-access')

    @cached_property
    def access_level_groups(self) -> dict[AccessLevel, Any]:
        return {al: self.create_group(al) for al in ACCESS_LEVELS}

    @staticmethod
    def get_pulumi_output_group_name(*, infra_name: str, dataset: str, kind: str):
        return f'{infra_name}-{dataset}-{kind}-group-id'

    def setup_web_access_group_memberships(self):
        self.web_access_group.add_member(
            self.infra.get_pulumi_name('web-access-group-access-group-membership'),
            member=self.analysis_group,
        )

    def setup_access_level_group_outputs(self):

        if isinstance(self.infra, DryRunInfra):
            return

        kinds = {
            'access': self.analysis_group,
            **self.access_level_groups,
        }

        for kind, group in kinds.items():
            pulumi.export(
                self.get_pulumi_output_group_name(
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
            group.add_member(
                self.infra.get_pulumi_name(
                    f'{kind}-{access_level}-access-level-group-membership'
                ),
                member=machine_account,
            )

    def setup_gcp_monitoring_access(self):
        assert isinstance(self.infra, GcpInfrastructure)

        self.infra.add_project_role(
            'project-compute-viewer',
            role='roles/compute.viewer',
            member=self.analysis_group,
            project=self.infra.project_id,
        )

        self.infra.add_project_role(
            'project-logging-viewer',
            role='roles/logging.viewer',
            member=self.analysis_group,
            project=self.infra.project_id,
        )

        self.infra.add_project_role(
            'project-monitoring-viewer',
            member=self.analysis_group,
            role='roles/monitoring.viewer',
        )

    # endregion ACCESS GROUPS
    # region STORAGE

    def setup_storage(self):
        if not self.should_setup_storage:
            return

        self.setup_storage_common_test_access()
        self.infra.give_member_ability_to_list_buckets(
            'project-buckets-lister', self.analysis_group
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

        self.setup_storage_outputs()

    def setup_storage_common_test_access(self):
        if self.dataset_config.dataset != self.config.reference_dataset:
            return

        self.infra.add_member_to_bucket(
            self.dataset_config.dataset + '-test-accessing-main',
            bucket=self.main_bucket,
            member=self.access_level_groups['test'],
            membership=BucketMembership.READ,
        )

    def setup_storage_outputs(self):

        buckets = {
            'main': {
                'default': self.infra.bucket_output_path(self.main_bucket),
                'web': self.infra.bucket_output_path(self.main_web_bucket),
                'analysis': self.infra.bucket_output_path(self.main_analysis_bucket),
                'tmp': self.infra.bucket_output_path(self.main_tmp_bucket),
                'upload': self.infra.bucket_output_path(
                    self.main_upload_buckets['main-upload']
                ),
                'web_url': self.config.web_url_template.format(
                    namespace='main', dataset=self.dataset_config.dataset
                ),
            },
            'test': {
                'default': self.infra.bucket_output_path(self.test_bucket),
                'web': self.infra.bucket_output_path(self.test_web_bucket),
                'analysis': self.infra.bucket_output_path(self.test_analysis_bucket),
                'tmp': self.infra.bucket_output_path(self.test_tmp_bucket),
                'upload': self.infra.bucket_output_path(self.test_upload_bucket),
                'web_url': self.config.web_url_template.format(
                    namespace='test', dataset=self.dataset_config.dataset
                ),
            },
        }

        stacks_to_reference = self.root.dataset_infrastructure[self.infra.name()]
        for namespace, al_buckets in buckets.items():

            configs_to_merge = []
            for dependent_dataset in self.dataset_config.depends_on:
                if config := stacks_to_reference[dependent_dataset].storage_tomls.get(
                    namespace
                ):
                    configs_to_merge.append(config)

            prepare_config_kwargs = {}
            if configs_to_merge:
                # Merge them here, because we have to pass it as a single
                # keyword-argument to Pulumi so we can reference it, but Pulumi
                # won't resolve a List[Output[T]]
                prepare_config_kwargs['_extra_configs'] = pulumi.Output.all(
                    *configs_to_merge
                ).apply(TOML_CONFIG_JOINER.join)

            if namespace == 'main':
                prepare_config_kwargs.update(
                    {
                        f'{ns}-{cat}': _bucket
                        for ns, ns_buckets in buckets.items()
                        for cat, _bucket in ns_buckets.items()
                    }
                )

                def _pulumi_prepare_function(arg):
                    """Redefine like this as Pulumi drops the self somehow"""
                    return self._pulumi_prepare_storage_outputs_main_function(arg)

            else:
                prepare_config_kwargs.update(al_buckets)

                def _pulumi_prepare_function(arg):
                    return self._pulumi_prepare_storage_outputs_test_function(arg)

            # This is an pulumi.Output[String]
            dataset_storage_config = pulumi.output.Output.all(
                **prepare_config_kwargs
            ).apply(_pulumi_prepare_function)

            # this export is important, it's how direct dependencies will be able to
            # access the nested dependencies, this export is potentially depending
            # on transitive dependencies.
            self.storage_tomls[namespace] = dataset_storage_config
            self.add_config_toml_to_bucket(
                namespace=namespace, contents=dataset_storage_config
            )

    def add_config_toml_to_bucket(self, namespace, contents: pulumi.Output):
        """
        Write the config to a bucket, this function decides the output-path based
        on the current deploy infra, dataset, access_level.
        :param namespace: test / main
        :param contents: some Pulumi awaitable string
        """
        if not self.config.config_destination:
            return

        if isinstance(self.infra, DryRunInfra):
            # we're likely not running in the pulumi engine,
            # so skip this step
            return

        _infra_to_call_function_on = None
        infra_prefix_map = [
            ('gs://', GcpInfrastructure),
            ('hail-az://', AzureInfra),
        ]
        for prefix, I in infra_prefix_map:
            if self.config.config_destination.startswith(prefix):
                _infra_to_call_function_on = (
                    self.infra
                    if isinstance(self.infra, I)
                    else I(self.config, self.dataset_config)
                )
                break
        else:
            raise ValueError(
                f'Could not find infra to save blob to for config_destination: '
                f'{self.config.config_destination}'
            )

        bucket_name, suffix = self.config.config_destination[len('gs://') :].split(
            '/', maxsplit=1
        )

        name = f'{self.infra.name()}-{self.dataset_config.dataset}-{namespace}'
        output_name = os.path.join(
            suffix,
            f'{self.infra.name()}/{self.dataset_config.dataset}-{namespace}' + '.toml',
        )

        _infra_to_call_function_on.add_blob_to_bucket(
            resource_name=f'storage-config-{name}',
            bucket=bucket_name,
            output_name=output_name,
            contents=contents,
        )

    def _pulumi_prepare_storage_outputs_test_function(self, arg):
        """
        Don't call this directly from Pulumi, as it strips the self
        """
        kwargs = dict(arg)
        config_dict = {}
        if '_extra_configs' in kwargs:
            for config_str in kwargs.pop('_extra_configs').split(TOML_CONFIG_JOINER):
                cpg_utils.config.update_dict(config_dict, toml.loads(config_str))

        storage_dict = {
            'storage': {'default': kwargs, self.dataset_config.dataset: kwargs}
        }
        if config_dict:
            cpg_utils.config.update_dict(config_dict, storage_dict)
        else:
            config_dict = storage_dict

        d = toml.dumps(config_dict)
        return d

    def _pulumi_prepare_storage_outputs_main_function(self, arg):
        kwargs = dict(arg)
        config_dict = {}
        if '_extra_configs' in kwargs:
            for config_str in kwargs.pop('_extra_configs').split(TOML_CONFIG_JOINER):
                cpg_utils.config.update_dict(config_dict, toml.loads(config_str))

        test_buckets = {
            name.removeprefix('test-'): bucket_path
            for name, bucket_path in kwargs.items()
            if name.startswith('test-')
        }
        main_buckets = {
            name.removeprefix('main-'): bucket_path
            for name, bucket_path in kwargs.items()
            if name.startswith('main-')
        }

        obj = {**main_buckets, 'test': test_buckets}
        storage_dict = {
            'storage': {
                'default': obj,
                self.dataset_config.dataset: obj,
            },
        }
        if config_dict:
            cpg_utils.config.update_dict(config_dict, storage_dict)
        else:
            config_dict = storage_dict

        d = toml.dumps(config_dict)
        return d

    def setup_storage_gcp_requester_pays_access(self):
        """
        Allows the usage of requester-pays buckets for
        access + test + standard + full groups
        :return:
        """
        assert isinstance(self.infra, GcpInfrastructure)

        kinds = {
            'access-group': self.analysis_group,
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
            self.analysis_group,
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
            self.analysis_group,
            BucketMembership.READ,
        )

        # web-server
        if isinstance(self.infra, GcpInfrastructure):
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
                member=self.analysis_group,
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
                (f'access-group-{bucket_name}-bucket-admin', self.analysis_group),
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
        if isinstance(self.infra, GcpInfrastructure):
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
            self.analysis_group,
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
        keys = {'access-group': self.analysis_group, **self.access_level_groups}

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
                'test': self.dataset_config.gcp.hail_service_account_test,
                'standard': self.dataset_config.gcp.hail_service_account_standard,
                'full': self.dataset_config.gcp.hail_service_account_full,
            }
        elif isinstance(self.infra, AzureInfra):
            assert (
                self.dataset_config.azure
            ), 'dataset_config.azure is required to be set'
            accounts = {
                'test': self.dataset_config.azure.hail_service_account_test,
                'standard': self.dataset_config.azure.hail_service_account_standard,
                'full': self.dataset_config.azure.hail_service_account_full,
            }
        else:
            return {}
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
                if hail_service_account := self.hail_accounts_by_access_level.get(
                    access_level
                ):
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
            # premade google group, so don't manage this one
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
                member=self.analysis_group,
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
    def sample_metadata_groups(
        self,
    ) -> dict[str, CPGInfrastructure.GroupProvider.Group]:
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
            member=self.analysis_group,
        )

    def setup_sample_metadata_access_permissions(self):
        if not self.should_setup_sample_metadata:
            return
        sm_access_levels: list[SampleMetadataAccessorMembership] = [
            SampleMetadataAccessorMembership(
                name='human',
                member=self.analysis_group,
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
                self.sample_metadata_groups[kind].add_member(
                    self.infra.get_pulumi_name(
                        f'sample-metadata-{kind}-{name}-access-level-group-membership'
                    ),
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
        accounts = {'access': self.analysis_group, **self.access_level_groups}
        for kind, account in accounts.items():
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
            'access-group': self.analysis_group,
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
            'notebook-account-users',
            machine_account=self.notebook_account,
            member=self.analysis_group,
        )

        # Grant the notebook account the same permissions as the access group members.
        self.analysis_group.add_member(
            self.infra.get_pulumi_name('notebook-service-account-access-group-member'),
            member=self.notebook_account,
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
            member=self.analysis_group,
        )

    def setup_analysis_runner_config_access(self):
        keys = {'access-group': self.analysis_group, **self.access_level_groups}

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
            f'shared-budget',
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

    # endregion ACCESS GROUP CACHE
    # region DEPENDENCIES

    def setup_dependencies(self):
        self.setup_dependencies_group_memberships()

    def setup_dependencies_group_memberships(self):

        # duplicate reference to avoid mutating config
        dependencies = list(self.dataset_config.depends_on)

        if (
            self.config.reference_dataset
            and self.dataset_config.dataset != self.config.reference_dataset
        ):
            dependencies.append(self.config.reference_dataset)

        stacks = self.root.dataset_infrastructure[self.infra.name()]
        for dependency in dependencies:

            # Adding dependent groups in two ways for reference:

            # 1. Grab the dependent stack, use the access_group member
            #    and add directly.
            stacks[dependency].analysis_group.add_member(
                resource_key=self.infra.get_pulumi_name(f'{dependency}-analysis-group'),
                member=self.analysis_group,
            )

            for access_level, primary_access_group in self.access_level_groups.items():

                # 2. Use the group provider to grab the group directly
                self.group_provider.get_group(
                    self.infra.name(), f'{dependency}-{access_level}'
                ).add_member(
                    self.infra.get_pulumi_name(
                        f'{dependency}-{access_level}-access-level-group'
                    ),
                    member=primary_access_group,
                )

    # endregion DEPENDENCIES
    # region UTILS

    @staticmethod
    def _get_name_from_external_sa(email: str, suffix='.iam.gserviceaccount.com'):
        """
        Convert service account email to name + some filtering.

        >>> CPGDatasetInfrastructure._get_name_from_external_sa('my-service-account@project.iam.gserviceaccount.com')
        'my-service-account-project'

        >>> CPGDatasetInfrastructure._get_name_from_external_sa('yourname@populationgenomics.org.au')
        'yourname'

        >>> CPGDatasetInfrastructure._get_name_from_external_sa('my.service-account+extra@domain.com')
        'my-service-account-extra'
        """
        if email.endswith(suffix):
            base = email[: -len(suffix)]
        else:
            base = email.split('@')[0]

        return NON_NAME_REGEX.sub('-', base).replace('--', '-')

    # endregion UTILS


def test():
    infra_config_dict = dict(cpg_utils.config.get_config())
    infra_config_dict['infrastructure']['reference_dataset'] = None
    infra_config = CPGInfrastructureConfig.from_dict(infra_config_dict)

    configs = [
        CPGDatasetConfig(
            dataset='fewgenomes',
            deploy_locations=['dry-run'],
            gcp=CPGDatasetConfig.Gcp(
                project='test-project',
                hail_service_account_test='fewgenomes-test@service-account',
                hail_service_account_standard='fewgenomes-standard@service-account',
                hail_service_account_full='fewgenomes-full@service-account',
            ),
        )
    ]
    infra = CPGInfrastructure(infra_config, configs)
    infra.main()


if __name__ == '__main__':
    test()
