# flake8: noqa: PGH003,ANN204,C901,ERA001,ANN401,SIM102
"""
CPG Dataset infrastructure
"""
import graphlib
import os.path
import re
from collections import defaultdict
from functools import cached_property
from typing import Any, Callable, Iterable, Iterator, NamedTuple, Type

import pulumi
import pulumi_gcp as gcp
import toml
import xxhash
from toml_sort import TomlSort

import cpg_utils.config
from cpg_infra.abstraction.azure import AzureInfra
from cpg_infra.abstraction.base import (
    BucketMembership,
    CloudInfraBase,
    ContainerRegistryMembership,
    DryRunInfra,
    MachineAccountRole,
    SecretMembership,
)
from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.abstraction.hailbatch import (
    HailBatchBillingProject,
    HailBatchBillingProjectMembership,
)
from cpg_infra.abstraction.metamist import MetamistProject, MetamistProjectMembers
from cpg_infra.config import (
    CloudName,
    CPGDatasetComponents,
    CPGDatasetConfig,
    CPGInfrastructureConfig,
    CPGInfrastructureUser,
    HailAccount,
)
from cpg_infra.plugin import get_plugins


class SampleMetadataAccessorMembership(NamedTuple):
    name: str
    member: Any
    permissions: Iterable[str]


SM_TEST_READ = 'test-read'
SM_TEST_WRITE = 'test-write'
SM_MAIN_READ = 'main-read'
SM_MAIN_WRITE = 'main-write'
METAMIST_PERMISSIONS = [
    SM_TEST_READ,
    SM_TEST_WRITE,
    SM_MAIN_READ,
    SM_MAIN_WRITE,
]


AccessLevel = str


def access_levels(*, include_test: bool) -> Iterable[AccessLevel]:
    if include_test:
        return ('test', 'standard', 'full')
    return ('standard', 'full')


NON_NAME_REGEX = re.compile(r'[^A-Za-z\d_-]')
TOML_CONFIG_JOINER = '\n||||'

NAME_TO_INFRA_CLASS: dict[str, Type[CloudInfraBase]] = {
    c.name(): c for c in CloudInfraBase.__subclasses__()  # type: ignore
}


def dict_to_toml(d: dict) -> str:
    """
    Convert dictionary to a sorted (and stable) TOML
    """
    # there's not an easy way to convert dictionary to the
    # internal tomlkit.TOMLDocument, as it has its own parser,
    # so let's just easy dump to string, to use the library from there.
    return TomlSort(toml.dumps(d)).sorted()


class CPGInfrastructure:
    """Class for managing all CPG infrastructure"""

    class GroupProvider:
        """Provider for managing groups + memberships"""

        class Group:
            """Placeholder for a Group of members"""

            class GroupMember:
                """
                Store both the username / cloud_id, as it's useful
                to look it up when resolving, ie for Hail Batch

                """

                def __init__(
                    self,
                    cloud_id: str,
                    user: CPGInfrastructureUser.Cloud | None,
                ) -> None:
                    self.cloud_id = cloud_id
                    self.user = user

                def __lt__(
                    self,
                    other: 'CPGInfrastructure.GroupProvider.Group.GroupMember',
                ):
                    return self.cloud_id < other.cloud_id

                def __repr__(self) -> str:
                    members = [
                        f'cloud_id={self.cloud_id!r}',
                    ]
                    if self.user:
                        members.append(f'username={self.user.id!r}')

                    return f'GroupMember({", ".join(members)})'

            # useful for checking isinstance without isinstance
            is_group = True

            def __init__(
                self,
                name: str,
                group: 'CPGInfrastructure.GroupProvider.Group',
                members: dict,
                cache_members: bool,
            ) -> None:
                self.name: str = name
                self.group: CPGInfrastructure.GroupProvider.Group = group
                self.cache_members: bool = cache_members
                self.members: dict[
                    str,
                    CPGInfrastructure.GroupProvider.Group.GroupMember
                    | CPGInfrastructure.GroupProvider.Group,
                ] = members

            def add_member(
                self,
                resource_key: str,
                member: 'str | CPGInfrastructure.GroupProvider.Group',
                user: CPGInfrastructureUser.Cloud | None = None,
            ):
                if isinstance(member, type(self)) and member.name == self.name:
                    raise ValueError(f'Cannot add self to group {self.name}')

                if isinstance(member, CPGInfrastructure.GroupProvider.Group):
                    self.members[resource_key] = member
                elif isinstance(user, CPGInfrastructureUser.Cloud):
                    self.members[resource_key] = self.GroupMember(member, user)
                else:
                    if user:
                        raise ValueError(
                            f'Invalid user type {type(user)} ({user}) for member '
                            f'{member} for {resource_key}',
                        )
                    self.members[resource_key] = self.GroupMember(member, None)

            def __repr__(self) -> str:
                return f'Group({self.name!r})'

        def __init__(self, group_prefix: str | None = None) -> None:
            self.groups: dict[
                str,
                dict[str, CPGInfrastructure.GroupProvider.Group],
            ] = defaultdict()

            self.group_prefix = group_prefix or ''
            self._cached_resolved_members: dict[str, list] = {}

        def get_group(self, infra_name: str, group_name: str):
            return self.groups[infra_name][group_name]

        def create_group(
            self,
            infra: CloudInfraBase,
            name: str,
            cache_members: bool,
            members: dict | None = None,
        ) -> Group:
            if infra.name() not in self.groups:
                self.groups[infra.name()] = {}
            if name in self.groups[infra.name()]:
                raise ValueError(f'Group "{name}" in "{infra.name()}" already exists')

            group = CPGInfrastructure.GroupProvider.Group(
                name=name,
                cache_members=cache_members,
                members=members or {},
                group=infra.create_group(self.group_prefix + name),
            )
            self.groups[infra.name()][name] = group

            return group

        def static_group_order(self, cloud: CloudName) -> list[Group]:
            """
            not that it super matters because we do recursively look it up and
            cache the result, but it's nice to grab the groups in an order that
            minimises depth looking.
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

        def resolve_group_members(
            self,
            group: 'Group',
        ) -> list['CPGInfrastructure.GroupProvider.Group.GroupMember']:
            if group.name in self._cached_resolved_members:
                return self._cached_resolved_members[group.name]

            resolved_members: list[
                CPGInfrastructure.GroupProvider.Group.GroupMember
            ] = []
            for member in group.members.values():
                if isinstance(member, CPGInfrastructure.GroupProvider.Group):
                    resolved_members.extend(self.resolve_group_members(member))
                else:
                    resolved_members.append(member)

            self._cached_resolved_members[group.name] = list(set(resolved_members))
            return resolved_members

    def __init__(
        self,
        config: CPGInfrastructureConfig,
        dataset_configs: list[CPGDatasetConfig],
    ) -> None:
        self.config = config
        self.dataset_configs: dict[str, CPGDatasetConfig] = {
            d.dataset: d for d in dataset_configs
        }

        self.group_provider = CPGInfrastructure.GroupProvider(
            group_prefix=self.config.group_prefix,
        )

        self.dataset_infrastructures: dict[
            str,
            CPGDatasetInfrastructure,
        ] = defaultdict()

    @cached_property
    def common_dataset(self) -> 'CPGDatasetInfrastructure':
        # ensure it's setup
        self.setup_datasets()
        return self.dataset_infrastructures[self.config.common_dataset]

    @cached_property
    def common_gcp_infra(self) -> GcpInfrastructure:
        return self.common_dataset.clouds[
            GcpInfrastructure.name()
        ].infra  # type: ignore

    @cached_property
    def common_azure_infra(self) -> AzureInfra:
        return self.common_dataset.clouds[AzureInfra.name()].infra  # type: ignore

    def resolve_dataset_order(self):
        """
        This isn't strictly required to deploy as resources aren't dependent,
        but sometimes is a useful exercise to sort resources because I *think*
        it influences the order pulumi uses to deploy.
        """
        reference_dataset = (
            [self.config.common_dataset] if self.config.common_dataset else []
        )
        deps = {
            k: v.depends_on + v.depends_on_readonly + reference_dataset
            for k, v in self.dataset_configs.items()
        }
        if self.config.common_dataset:
            deps[self.config.common_dataset] = []

        return graphlib.TopologicalSorter(deps).static_order()

    def main(self):
        # Go through each dataset and instantiate the CPGDatasetInfrastructure class
        # for that dataset.
        self.setup_datasets()

        # create a bucket and attach accessor members to it. The bucket itself is
        # created by accessing the property `self.gcp_members_cache_bucket`
        # This will also have the side effect of creating a cloud resource manager and
        # identity service
        self.setup_gcp_access_cache_bucket()

        # creates the group metamist-invokers group and gives it invoker permissions
        # to the cloud run service specified in infrastructure.metamist.gcp.service_name
        self.setup_gcp_metamist_cloudrun_invoker()

        # Create a python registry for storing private python packages
        self.setup_python_registry()

        # Deploy all the assets required for each dataset. Groups, permissions
        # storage buckets, metamist and hail users etc.
        self.deploy_datasets()

        plugins = get_plugins()
        for plugin_name in self.config.plugins_enabled:
            if plugin_name not in plugins:
                raise Exception(f"Plugin `{plugin_name}` is not installed")

            plugins[plugin_name](self, self.config).main()

        # Up to this point the groups have not actually been created, go through
        # the groups data structure and create the necessary groups in the correct
        # order so that group dependencies can be handled
        self.finalize_groups()

        self.setup_hail_batch_billing_project_members()

        # Add read and write level members to metamist projects
        self.update_metamist_members()

        # Store the deployed infrastructure config on gcp storage
        self.output_infrastructure_config()

    def setup_datasets(self):
        if self.dataset_infrastructures:
            # don't do this repeatedly
            return
        for dataset in self.resolve_dataset_order():
            self.dataset_infrastructures[dataset] = CPGDatasetInfrastructure(
                root=self,
                config=self.config,
                dataset_config=self.dataset_configs[dataset],
                group_provider=self.group_provider,
            )

    def deploy_datasets(self):
        for cloud_dataset in self.dataset_infrastructures.values():
            cloud_dataset.main()

    def setup_hail_batch_billing_project_members(self):
        internal_users = [
            user
            for user in self.config.users.values()
            if user.add_to_internal_hail_batch_projects
        ]

        for dataset_infra in self.dataset_infrastructures.values():
            for cloud, dataset_cloud_infra in dataset_infra.clouds.items():
                if not dataset_cloud_infra.should_setup_hail:
                    continue

                infra = dataset_cloud_infra.infra

                if self.config.billing.hail_aggregator_username:
                    HailBatchBillingProjectMembership(
                        infra.get_pulumi_name(
                            'batch-billing-member-billing-aggregator',
                        ),
                        billing_project=dataset_cloud_infra.hail_batch_billing_project,
                        user=self.config.billing.hail_aggregator_username,
                    )

                for (
                    name,
                    hail_account,
                ) in dataset_cloud_infra.hail_accounts_by_access_level.items():
                    HailBatchBillingProjectMembership(
                        infra.get_pulumi_name(f'batch-billing-member-hail-{name}'),
                        billing_project=dataset_cloud_infra.hail_batch_billing_project,
                        user=hail_account.username,
                    )

                _group_members = self.group_provider.resolve_group_members(
                    dataset_cloud_infra.analysis_group,
                )
                hail_batch_usernames = {
                    m.user.hail_batch_username
                    for m in _group_members
                    if m.user and m.user.hail_batch_username
                }
                if dataset_infra.dataset_config.is_internal_dataset:
                    hail_batch_usernames.update(
                        user.clouds[cloud].hail_batch_username
                        for user in internal_users
                        if cloud in user.clouds
                        and user.clouds[cloud].hail_batch_username
                    )

                def _make_add_member_function(
                    _data_provider: 'CPGDatasetCloudInfrastructure',
                    _infra: CloudInfraBase,
                ) -> Callable[[list[str]], None]:
                    # bind loop variables so they're available in
                    # the functional context below

                    def _add_member_to_billing_project(
                        _analysis_members: list[str],
                    ) -> None:
                        for hail_id in sorted(set(_analysis_members)):
                            if not isinstance(hail_id, str):
                                continue
                            try:
                                h = _data_provider.compute_hash(
                                    dataset=_data_provider.dataset_config.dataset,
                                    member=hail_id,
                                    cloud=_infra.name(),
                                )
                            except Exception as e:
                                print(f'Exception during hash calculation: {e}')
                                raise e

                            HailBatchBillingProjectMembership(
                                _infra.get_pulumi_name(f'batch-billing-member-{h}'),
                                billing_project=_data_provider.hail_batch_billing_project,
                                user=hail_id,
                            )

                    return _add_member_to_billing_project

                pulumi.Output.all(*hail_batch_usernames).apply(
                    _make_add_member_function(dataset_cloud_infra, infra),
                )

    @staticmethod
    def _email_key(m_: str) -> tuple[str, str]:
        """Sort on domain, then on name"""
        s = m_.split('@')
        return s[1], s[0]

    @staticmethod
    def sort_members(members: list[str]) -> list[str]:
        """Sort members on domain, then on name"""
        return sorted(
            {str(m).lower() for m in members},
            key=CPGInfrastructure._email_key,
        )

    def finalize_groups(self):
        # capture these variables so they don't change during the resolution period
        def _process_members(members: list[str]) -> str:
            distinct_users = CPGInfrastructure.sort_members(members)
            return '\n'.join(distinct_users)

        # now resolve groups
        for cloud in self.group_provider.groups:
            # We're adding groups, but it does rely on some service being created
            infra = self.common_dataset.clouds[cloud].infra

            for group in self.group_provider.static_group_order(cloud=cloud):
                for resource_key, member in group.members.items():
                    infra.add_group_member(
                        resource_key=resource_key,
                        group=group.group,
                        member=(
                            member.cloud_id
                            if isinstance(
                                member,
                                CPGInfrastructure.GroupProvider.Group.GroupMember,
                            )
                            else member.group
                        ),
                        unique_resource_key=True,
                    )

                if group.cache_members and isinstance(infra, GcpInfrastructure):
                    _members = self.group_provider.resolve_group_members(group)
                    member_ids = [infra.member_id(m.cloud_id) for m in _members]
                    members_contents = '\n'

                    if len(member_ids) > 0:
                        if all(isinstance(m, str) for m in member_ids):
                            members_contents = _process_members(member_ids) or '\n'
                        else:
                            members_contents = (
                                pulumi.Output.all(*member_ids)
                                .apply(_process_members)
                                .apply(lambda value: value or '\n')
                            )

                    # we'll create a blob with the members of the groups
                    infra.add_blob_to_bucket(
                        f'{group.name}-group-cache-members',
                        bucket=self.gcp_members_cache_bucket,
                        contents=members_contents,
                        output_name=f'{group.name}-members.txt',
                    )

    def update_metamist_members(self):
        """Send a request to metamist to update group members"""

        def prepare_group_members(
            dataset_infra: CPGDatasetInfrastructure,
            group_name: str,
        ) -> pulumi.Output[str]:
            # only add GCP accounts for now
            clouds = [GcpInfrastructure.name()]
            members: list[str | pulumi.Output[str]] = []
            for cloud_name in clouds:
                if cloud_name not in dataset_infra.clouds:
                    continue
                cloud_infra = dataset_infra.clouds[cloud_name]

                sm_groups = cloud_infra.metamist_groups
                if group_name not in sm_groups:
                    pulumi.warn(
                        f'{dataset_infra.dataset} :: metamist-group {group_name!r} '
                        'not in sm-groups',
                    )
                    continue
                cloud_members = self.group_provider.resolve_group_members(
                    sm_groups[group_name],
                )
                members.extend(
                    cloud_infra.infra.member_id(member.cloud_id)
                    for member in cloud_members
                )

            return pulumi.Output.all(*members).apply(CPGInfrastructure.sort_members)

        for dataset, infra in self.dataset_infrastructures.items():
            if not infra.dataset_config.enable_metamist_project:
                continue

            MetamistProjectMembers(
                f'{dataset}-metamist-members',
                metamist_project_name=infra.metamist_project.project_name,
                read_members=prepare_group_members(infra, SM_MAIN_READ),
                write_members=prepare_group_members(infra, SM_MAIN_WRITE),
            )

            if infra.dataset_config.setup_test:
                MetamistProjectMembers(
                    f'{dataset}-metamist-test-members',
                    metamist_project_name=infra.metamist_test_project.project_name,
                    read_members=prepare_group_members(infra, SM_TEST_READ),
                    write_members=prepare_group_members(infra, SM_TEST_WRITE),
                )

    # dataset agnostic infrastructure

    def build_infrastructure_config_output(self) -> dict[str, pulumi.Output[str] | str]:
        output: dict[str, pulumi.Output[str] | str] = {
            'members_cache_location': self.common_gcp_infra.bucket_output_path(
                self.gcp_members_cache_bucket,
            ),
        }
        if self.config.hail is not None:
            if self.config.hail.gcp.git_credentials_secret_name is not None:
                output[
                    'git_credentials_secret_name'
                ] = self.config.hail.gcp.git_credentials_secret_name
            if self.config.hail.gcp.git_credentials_secret_project is not None:
                output[
                    'git_credentials_secret_project'
                ] = self.config.hail.gcp.git_credentials_secret_project

        return output

    def output_infrastructure_config(self):
        # we'll only do it on GCP for now

        items = self.build_infrastructure_config_output().items()

        def _build_config(values: list) -> str:
            """Build config from pulumi awaited values"""
            keys = [v[0] for v in items]
            # nest in .infrastructure
            d = {'infrastructure': dict(zip(keys, values))}

            return dict_to_toml(d)

        infra_config = pulumi.Output.all(*[v[1] for v in items]).apply(_build_config)
        bucket_name, suffix = self.config.config_destination.removeprefix(
            'gs://',
        ).split('/', maxsplit=1)
        self.common_gcp_infra.add_blob_to_bucket(
            'infrastructure-config',
            bucket=bucket_name,
            contents=infra_config,
            output_name=os.path.join(suffix, 'infrastructure.toml'),
        )

    # region ACCESS_CACHE

    @cached_property
    def gcp_members_cache_bucket(self):
        bucket = self.common_gcp_infra.create_bucket(
            f'{self.config.gcp.dataset_storage_prefix}members-group-cache',
            unique=True,
            versioning=True,
            autoclass=False,  # Always accessed frequently.
            lifecycle_rules=[],
        )

        # run as a pulumi export, even though it's exported in the config
        pulumi.export(
            'members-cache-bucket',
            self.common_gcp_infra.bucket_output_path(bucket),
        )
        return bucket

    def setup_gcp_access_cache_bucket(self):
        group_cache_accessors = []

        if self.config.analysis_runner:
            group_cache_accessors.append(
                (
                    'analysis-runner',
                    self.config.analysis_runner.gcp.server_machine_account,
                ),
            )

        if self.config.metamist:
            group_cache_accessors.append(
                ('sample-metadata', self.config.metamist.gcp.machine_account),
            )

        if self.config.web_service:
            group_cache_accessors.append(
                ('web-service', self.config.web_service.gcp.server_machine_account),
            )

        for key, account in group_cache_accessors:
            self.common_gcp_infra.add_member_to_bucket(
                f'{key}-members-group-cache-accessor',
                bucket=self.gcp_members_cache_bucket,
                member=account,
                membership=BucketMembership.READ,
            )

    # endregion ACCESS_CACHE

    @cached_property
    def gcp_metamist_invoker_group(self):
        return self.group_provider.create_group(
            self.common_gcp_infra,
            cache_members=False,
            name='sample-metadata-invokers',
        )

    def setup_gcp_metamist_cloudrun_invoker(self):
        # pylint: disable
        infra = self.common_gcp_infra

        if not isinstance(infra, GcpInfrastructure):
            raise ValueError(
                f'Dataset_infrastructure for {self.config.common_dataset!r} was not of '
                f'type GCPInfrastructure, this is probably a bug',
            )

        assert self.config.metamist

        infra.add_cloudrun_invoker(
            'sample-metadata-cloudrun-invokers',
            service=self.config.metamist.gcp.service_name,
            project=self.config.metamist.gcp.project,
            member=self.gcp_metamist_invoker_group,
        )

    @cached_property
    def gcp_python_registry(self):
        """
        Create a registry for private python packages, we only need one for our org,
        andt there's no equivalent for Azure.

        """
        assert self.config.gcp
        assert self.common_gcp_infra.project

        return gcp.artifactregistry.Repository(
            'python-artifact-registry',
            repository_id='python-registry',
            project=self.common_gcp_infra.project.project_id,
            format='PYTHON',
            location=self.config.gcp.region,
            description='Python packages for CPG',
        )

    def setup_python_registry(self):
        """
        Setup the python registry permissions in gcp-common
        """
        # force the creation
        _ = self.gcp_python_registry


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
        dataset_config: CPGDatasetConfig,
    ) -> None:
        self.config = config
        self.root = root
        self.group_provider = group_provider

        self.dataset: str = dataset_config.dataset
        self.dataset_config: CPGDatasetConfig = dataset_config
        self.deploy_locations = dataset_config.deploy_locations

        self.clouds: dict[str, CPGDatasetCloudInfrastructure] = {
            deploy_location: CPGDatasetCloudInfrastructure(
                config=self.config,
                root=self.root,
                group_provider=self.group_provider,
                infra=NAME_TO_INFRA_CLASS[deploy_location](
                    config=self.config,
                    dataset_config=self.dataset_config,
                ),
                dataset_config=self.dataset_config,
            )
            for deploy_location in self.deploy_locations
        }

    def main(self):
        self.setup_metamist()

        for infra in self.clouds.values():
            infra.main()

    def setup_metamist(self):
        if self.dataset_config.enable_metamist_project:
            # setup metamist project by accessing the property
            _ = self.metamist_project
            if self.dataset_config.setup_test:
                _ = self.metamist_test_project

    @cached_property
    def metamist_project(self):
        return MetamistProject(
            f'metamist-project-{self.dataset}',
            project_name=self.dataset,
        )

    @cached_property
    def metamist_test_project(self):
        return MetamistProject(
            f'metamist-project-{self.dataset}-test',
            project_name=self.dataset + '-test',
        )


class CPGDatasetCloudInfrastructure:
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
    ) -> None:
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
        self.should_setup_metamist = CPGDatasetComponents.METAMIST in self.components
        self.should_setup_hail = CPGDatasetComponents.HAIL_ACCOUNTS in self.components
        self.should_setup_container_registry = (
            CPGDatasetComponents.CONTAINER_REGISTRY in self.components
        )
        self.should_setup_analysis_runner = (
            CPGDatasetComponents.ANALYSIS_RUNNER in self.components
        )

        # outputs
        self.storage_tomls: dict = {}

    def create_group(self, name: str, cache_members: bool = False):
        """
        Create a group with the dataset name as a prefix.

        :param name: name of the group, without the dataset prefix
        :param cache_members: whether to cache the members in a bucket
        """
        group_name = f'{self.dataset_config.dataset}-{name}'
        # group = self.infra.create_group(group_name)
        return self.group_provider.create_group(
            self.infra,
            cache_members=cache_members,
            name=group_name,
        )

    def main(self):
        self.setup_access_groups()
        self.setup_externally_specified_members()
        self.setup_billing()

        # optional components
        if self.should_setup_storage:
            self.setup_storage()
        if self.should_setup_metamist:
            self.setup_metamist()
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

        self.infra.finalise()

    # region MACHINE ACCOUNTS

    @cached_property
    def project(self):
        return self.infra.project

    @cached_property
    def main_upload_account(self):
        return self.infra.create_machine_account('main-upload')

    @cached_property
    def working_machine_accounts_by_type(
        self,
    ) -> dict[str, list[tuple[AccessLevel, Any]]]:
        machine_accounts: dict[str, list] = defaultdict(list)

        for access_level, account in self.hail_accounts_by_access_level.items():
            machine_accounts['hail'].append((access_level, account.cloud_id))
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

    def working_machine_accounts_by_access_level(self) -> dict[AccessLevel, list[Any]]:
        machine_accounts: dict[AccessLevel, list[Any]] = defaultdict(list)
        for _, values in self.working_machine_accounts_by_type.items():
            for access_level, machine_account in values:
                machine_accounts[access_level].append(machine_account)

        return machine_accounts

    @cached_property
    def deployment_accounts_by_access_level(self):
        accounts = {
            'standard': self.dataset_config.deployment_service_account_standard,
            'full': self.dataset_config.deployment_service_account_full,
        }
        if self.dataset_config.setup_test:
            accounts['test'] = self.dataset_config.deployment_service_account_test
        return {k: v for k, v in accounts.items() if v}

    # endregion MACHINE ACCOUNTS

    # region PERSON ACCESS
    def setup_externally_specified_members(self):
        groups = [
            self.data_manager_group,
            self.analysis_group,
            self.metadata_access_group,
            self.upload_group,
            self.web_access_group,
            self.release_access_group,
        ]

        for group in groups:
            group_name = group.name.removeprefix(self.dataset_config.dataset + '-')
            for member_id in self.dataset_config.members.get(group_name, []):
                member = self.config.users.get(member_id)
                if not member:
                    raise ValueError(f'Member {member_id} not found in config')

                h = self.compute_hash(
                    dataset=self.dataset_config.dataset,
                    member=member.id,
                    cloud=self.infra.name(),
                )
                if cloud_user := member.clouds[self.infra.name()]:
                    group.add_member(
                        self.infra.get_pulumi_name(f'{group.name}-member-{h}'),
                        member=cloud_user.id,
                        user=cloud_user,
                    )

    @staticmethod
    def compute_hash(dataset: str, member: str, cloud: str) -> str:
        """
        >>> CPGDatasetCloudInfrastructure.compute_hash('dataset', 'hello.world@email.com', '')
        'HW-d51b65ee'
        """
        initials = ''.join(n[0] for n in member.split('@')[0].split('.')).upper()
        # I was going to say "add a salt", but we're displaying the initials,
        # so let's call it something like salt, monosodium glutamate ;)
        msg = dataset + member + cloud
        computed_hash = xxhash.xxh32(msg.encode()).hexdigest()
        return initials + '-' + computed_hash

    # endregion

    # region BILLING

    def setup_billing(self):
        if not isinstance(self.infra, GcpInfrastructure):
            # pass here for now, as budgets are not well implemented on Azure yet
            return

        budget = self.dataset_config.budgets.get(self.infra.name())
        if not budget:
            raise ValueError(
                f'No budget for {self.dataset_config.dataset}.{self.infra.name()}',
            )

        self.infra.create_monthly_budget('monthly-budget', budget=budget.monthly_budget)

    # endregion BILLING

    # region ACCESS GROUPS

    def setup_access_groups(self):
        self.setup_web_access_group_memberships()
        self.setup_access_level_group_memberships()
        self.setup_dependencies_group_memberships()

        # transitive person groups
        self.analysis_group.add_member(
            self.infra.get_pulumi_name('data-manager-in-analysis'),
            self.data_manager_group,
        )
        self.upload_group.add_member(
            self.infra.get_pulumi_name('data-manager-in-upload'),
            self.data_manager_group,
        )
        self.metadata_access_group.add_member(
            self.infra.get_pulumi_name('analysis-in-metadata'),
            self.analysis_group,
        )
        self.web_access_group.add_member(
            self.infra.get_pulumi_name('metadata-in-web-access'),
            self.metadata_access_group,
        )

        # transitive storage groups
        if self.dataset_config.setup_test:
            self.test_read_group.add_member(
                self.infra.get_pulumi_name('test-full-in-test-read'),
                self.test_full_group,
            )
            self.test_full_group.add_member(
                self.infra.get_pulumi_name('analysis-group-in-test-full'),
                self.analysis_group,
            )
            self.test_full_group.add_member(
                self.infra.get_pulumi_name('full-in-test-full'),
                self.full_group,
            )
            self.test_full_group.add_member(
                self.infra.get_pulumi_name('test-in-test-full'),
                self.test_group,
            )

        self.main_list_group.add_member(
            self.infra.get_pulumi_name('analysis-group-in-main-list'),
            self.analysis_group,
        )

        self.main_read_group.add_member(
            self.infra.get_pulumi_name('main-create-in-main-read'),
            self.main_create_group,
        )
        self.main_read_group.add_member(
            self.infra.get_pulumi_name('data-manager-in-main-read'),
            self.data_manager_group,
        )
        self.main_create_group.add_member(
            self.infra.get_pulumi_name('standard-in-main-create'),
            self.standard_group,
        )
        self.main_create_group.add_member(
            self.infra.get_pulumi_name('full-in-main-create'),
            self.full_group,
        )

        if isinstance(self.infra, GcpInfrastructure):
            self.setup_gcp_monitoring_access()

    @cached_property
    def data_manager_group(self):
        return self.create_group('data-manager')

    @cached_property
    def images_reader_group(self):
        return self.create_group('images-reader')

    @cached_property
    def images_writer_group(self):
        return self.create_group('images-writer')

    @cached_property
    def analysis_group(self):
        return self.create_group('analysis', cache_members=True)

    @cached_property
    def metadata_access_group(self):
        return self.create_group('metadata-access')

    @cached_property
    def web_access_group(self):
        return self.create_group('web-access', cache_members=True)

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

    # access groups

    @cached_property
    def test_read_group(self):
        return self.create_group('test-read')

    @cached_property
    def test_full_group(self):
        return self.create_group('test-full')

    @cached_property
    def main_list_group(self):
        return self.create_group('main-list')

    @cached_property
    def main_read_group(self):
        return self.create_group('main-read')

    @cached_property
    def main_create_group(self):
        return self.create_group('main-create')

    @cached_property
    def test_group(self):
        return self.create_group('test')

    @cached_property
    def standard_group(self):
        return self.create_group('standard')

    @cached_property
    def full_group(self):
        return self.create_group('full')

    @cached_property
    def access_level_groups(self) -> dict[AccessLevel, Any]:
        accounts = {
            'standard': self.standard_group,
            'full': self.full_group,
        }
        if self.dataset_config.setup_test:
            accounts['test'] = self.test_group
        return accounts

    @staticmethod
    def get_pulumi_output_group_name(
        *,
        infra_name: str,
        dataset: str,
        kind: str,
    ) -> str:
        return f'{infra_name}-{dataset}-{kind}-group-id'

    def setup_web_access_group_memberships(self):
        self.web_access_group.add_member(
            self.infra.get_pulumi_name('analysis-in-web-access'),
            member=self.analysis_group,
            user=None,
        )

    def setup_access_level_group_memberships(self):
        for (
            kind,
            access_level,
            machine_account,
        ) in self.working_machine_accounts_kind_al_account_gen():
            if group := self.access_level_groups.get(access_level):
                group.add_member(
                    self.infra.get_pulumi_name(
                        f'{kind}-{access_level}-access-level-group-membership',
                    ),
                    member=machine_account,
                )
            else:
                print(
                    f'No access level group for {access_level} in {self.dataset_config.dataset}',
                )

    def setup_gcp_monitoring_access(self):
        assert isinstance(self.infra, GcpInfrastructure)

        self.infra.add_project_role(
            'project-compute-viewer',
            role='roles/compute.viewer',
            member=self.analysis_group.group,
            project=self.infra.project_id,
        )

        self.infra.add_project_role(
            'project-logging-viewer',
            role='roles/logging.viewer',
            member=self.analysis_group.group,
            project=self.infra.project_id,
        )

        self.infra.add_project_role(
            'project-monitoring-viewer',
            member=self.analysis_group.group,
            role='roles/monitoring.viewer',
        )

    # endregion ACCESS GROUPS
    # region STORAGE

    def setup_storage(self):
        if not self.should_setup_storage:
            return

        self.infra.give_member_ability_to_list_buckets(
            'project-buckets-lister',
            self.main_list_group,
        )
        self.setup_storage_archive_bucket_permissions()
        self.setup_storage_main_bucket_permissions()
        self.setup_storage_main_tmp_bucket()
        self.setup_storage_main_analysis_bucket()
        self.setup_storage_main_web_bucket_permissions()
        self.setup_storage_main_upload_buckets_permissions()

        if self.dataset_config.setup_test:
            self.setup_storage_common_test_access()  # extra access for common dataset
            self.setup_storage_test_buckets_permissions()

        if self.dataset_config.enable_release:
            self.setup_storage_release_bucket_permissions()

        if isinstance(self.infra, GcpInfrastructure):
            self.setup_storage_gcp_requester_pays_access()
            self.infra.add_member_to_machine_account_role(
                'data-manager-credentials-generator',
                machine_account=self.main_upload_account,
                member=self.data_manager_group,
                role=MachineAccountRole.CREDENTIALS_ADMIN,
            )

            self.infra.add_project_role(
                'data-manager-project-iam-viewer',
                member=self.data_manager_group,
                role='roles/iam.roleViewer',
            )

        self.setup_storage_outputs()

    def setup_storage_common_test_access(self):
        if self.dataset_config.dataset != self.config.common_dataset:
            return

        self.infra.add_member_to_bucket(
            self.dataset_config.dataset + '-test-accessing-main',
            bucket=self.main_bucket,
            member=self.test_read_group,
            membership=BucketMembership.READ,
        )

    def setup_storage_outputs(self):
        web_url_template = (
            self.config.web_service.web_url_template
            if self.config.web_service
            else None
        )

        buckets = {
            'main': {
                'default': self.infra.bucket_output_path(self.main_bucket),
                'web': self.infra.bucket_output_path(self.main_web_bucket),
                'analysis': self.infra.bucket_output_path(self.main_analysis_bucket),
                'tmp': self.infra.bucket_output_path(self.main_tmp_bucket),
                'upload': self.infra.bucket_output_path(
                    self.main_upload_buckets['main-upload'],
                ),
            },
        }

        if web_url_template:
            buckets['main']['web_url'] = web_url_template.format(
                namespace='main',
                dataset=self.dataset_config.dataset,
            )

        if self.dataset_config.setup_test:
            buckets['test'] = {
                'default': self.infra.bucket_output_path(self.test_bucket),
                'web': self.infra.bucket_output_path(self.test_web_bucket),
                'analysis': self.infra.bucket_output_path(self.test_analysis_bucket),
                'tmp': self.infra.bucket_output_path(self.test_tmp_bucket),
                'upload': self.infra.bucket_output_path(self.test_upload_bucket),
            }

            if web_url_template:
                buckets['test']['web_url'] = web_url_template.format(
                    namespace='test',
                    dataset=self.dataset_config.dataset,
                )

        dependent_datasets = {
            *(self.dataset_config.depends_on or []),
            *(self.dataset_config.depends_on_readonly or []),
        }
        if self.dataset_config.dataset != self.config.common_dataset:
            dependent_datasets.add(self.config.common_dataset)

        stacks_to_reference = self.root.dataset_infrastructures
        for namespace, al_buckets in buckets.items():
            configs_to_merge = []
            for dependent_dataset in sorted(dependent_datasets):
                if cloud_infra := stacks_to_reference[dependent_dataset].clouds.get(
                    self.infra.name(),
                ):
                    if config := cloud_infra.storage_tomls.get(namespace):
                        configs_to_merge.append(config)

            prepare_config_kwargs = {}
            if configs_to_merge:
                # Merge them here, because we have to pass it as a single
                # keyword-argument to Pulumi so we can reference it, but Pulumi
                # won't resolve a List[Output[T]]
                prepare_config_kwargs['_extra_configs'] = pulumi.Output.all(
                    *configs_to_merge,
                ).apply(TOML_CONFIG_JOINER.join)

            if namespace == 'main':
                prepare_config_kwargs.update(
                    {
                        f'{ns}-{cat}': _bucket
                        for ns, ns_buckets in buckets.items()
                        for cat, _bucket in ns_buckets.items()
                    },
                )

                def _pulumi_prepare_function(arg):  # noqa: ANN001,ANN202
                    """Redefine like this as Pulumi drops the self somehow"""
                    return self._pulumi_prepare_storage_outputs_main_function(arg)

            else:
                prepare_config_kwargs.update(al_buckets)

                def _pulumi_prepare_function(arg):  # noqa: ANN001,ANN202
                    return self._pulumi_prepare_storage_outputs_test_function(arg)

            # This is a pulumi.Output[String]
            dataset_storage_config = pulumi.output.Output.all(
                **prepare_config_kwargs,
            ).apply(_pulumi_prepare_function)

            # this export is important, it's how direct dependencies will be able to
            # access the nested dependencies, this export is potentially depending
            # on transitive dependencies.
            self.storage_tomls[namespace] = dataset_storage_config
            self.add_config_toml_to_bucket(
                namespace=namespace,
                contents=dataset_storage_config,
            )

    def add_config_toml_to_bucket(self, namespace: str, contents: pulumi.Output):
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
        infra_prefix_map = [GcpInfrastructure, AzureInfra]
        for Infra in infra_prefix_map:  # noqa: N806
            if re.match(Infra.storage_url_regex(), self.config.config_destination):
                _infra_to_call_function_on = (
                    self.infra
                    if isinstance(self.infra, Infra)
                    else Infra(self.config, self.dataset_config)
                )
                break
        else:
            raise ValueError(
                f'Could not find infra to save blob to for config_destination: '
                f'{self.config.config_destination}',
            )

        bucket_name, suffix = self.config.config_destination.removeprefix(
            'gs://',
        ).split('/', maxsplit=1)

        name = f'{self.infra.name()}-{self.dataset_config.dataset}-{namespace}'
        output_name = os.path.join(
            suffix,
            'storage',
            f'{self.infra.name()}/{self.dataset_config.dataset}-{namespace}' + '.toml',
        )

        _infra_to_call_function_on.add_blob_to_bucket(
            resource_name=f'storage-config-{name}',
            bucket=bucket_name,
            output_name=output_name,
            contents=contents,
        )

    def _pulumi_prepare_storage_outputs_test_function(self, arg: Any) -> str:
        """
        Don't call this directly from Pulumi, as it strips the self
        """
        kwargs = dict(arg)
        config_dict: dict[str, Any] = {}
        if '_extra_configs' in kwargs:
            for config_str in kwargs.pop('_extra_configs').split(TOML_CONFIG_JOINER):
                cpg_utils.config.update_dict(config_dict, toml.loads(config_str))

        storage_dict = {
            'storage': {'default': kwargs, self.dataset_config.dataset: kwargs},
        }
        if config_dict:
            cpg_utils.config.update_dict(config_dict, storage_dict)
        else:
            config_dict = storage_dict

        return dict_to_toml(config_dict)

    def _pulumi_prepare_storage_outputs_main_function(self, arg: Any) -> str:
        kwargs: dict[str, Any] = dict(arg)
        config_dict: dict[str, Any] = {}
        if '_extra_configs' in kwargs:
            for config_str in kwargs.pop('_extra_configs').split(TOML_CONFIG_JOINER):
                cpg_utils.config.update_dict(config_dict, toml.loads(config_str))

        obj = {
            name.removeprefix('main-'): bucket_path
            for name, bucket_path in kwargs.items()
            if name.startswith('main-')
        }
        if self.dataset_config.setup_test:
            obj['test'] = {
                name.removeprefix('test-'): bucket_path
                for name, bucket_path in kwargs.items()
                if name.startswith('test-')
            }

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

        return dict_to_toml(config_dict)

    def setup_storage_gcp_requester_pays_access(self):
        """
        Allows the usage of requester-pays buckets for
        access + test + standard + full groups
        :return:
        """
        assert isinstance(self.infra, GcpInfrastructure)

        kinds = {
            'analysis-group': self.analysis_group,
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
            'main-list-archive-bucket',
            self.archive_bucket,
            self.main_list_group,
            BucketMembership.LIST,
        )
        self.infra.add_member_to_bucket(
            'full-archive-bucket-admin',
            self.archive_bucket,
            self.full_group,
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
            autoclass=False,  # Manually managed cold tier.
        )

    # region MAIN BUCKETS

    def setup_storage_main_bucket_permissions(self):
        # analysis already has list permission

        self.infra.add_member_to_bucket(
            'main-read-main-bucket-read',
            self.main_bucket,
            self.main_read_group,
            BucketMembership.READ,
        )

        self.infra.add_member_to_bucket(
            'main-create-main-bucket-view-create',
            self.main_bucket,
            self.main_create_group,
            BucketMembership.APPEND,
        )

        self.infra.add_member_to_bucket(
            'full-main-bucket-admin',
            self.main_bucket,
            self.full_group,
            BucketMembership.MUTATE,
        )

    def setup_storage_main_tmp_bucket(self):
        self.infra.add_member_to_bucket(
            'main-read-main-tmp-bucket-read',
            self.main_tmp_bucket,
            self.main_read_group,
            BucketMembership.READ,
        )

        self.infra.add_member_to_bucket(
            'main-create-main-tmp-bucket-view-create',
            self.main_tmp_bucket,
            self.main_create_group,
            BucketMembership.APPEND,
        )

        self.infra.add_member_to_bucket(
            'full-main-tmp-bucket-admin',
            self.main_tmp_bucket,
            self.full_group,
            BucketMembership.MUTATE,
        )

    def setup_storage_main_analysis_bucket(self):
        self.infra.add_member_to_bucket(
            'analysis-group-main-analysis-bucket-viewer',
            self.main_analysis_bucket,
            self.analysis_group,
            BucketMembership.READ,
        )

        self.infra.add_member_to_bucket(
            'main-read-main-analysis-bucket-viewer',
            self.main_analysis_bucket,
            self.main_read_group,
            BucketMembership.READ,
        )
        self.infra.add_member_to_bucket(
            'main-create-main-analysis-bucket-view-create',
            self.main_analysis_bucket,
            self.main_create_group,
            BucketMembership.APPEND,
        )

        self.infra.add_member_to_bucket(
            'full-main-analysis-bucket-admin',
            self.main_analysis_bucket,
            self.full_group,
            BucketMembership.MUTATE,
        )

    def setup_storage_main_web_bucket_permissions(self):
        self.infra.add_member_to_bucket(
            'analysis-group-main-web-bucket-viewer',
            self.main_web_bucket,
            self.analysis_group,
            BucketMembership.READ,
        )

        # web-server
        if (
            isinstance(self.infra, GcpInfrastructure)
            and self.config.web_service is not None
        ):
            self.infra.add_member_to_bucket(
                'web-server-main-web-bucket-viewer',
                self.main_web_bucket,
                self.config.web_service.gcp.server_machine_account,  # WEB_SERVER_SERVICE_ACCOUNT,
                BucketMembership.READ,
            )

        self.infra.add_member_to_bucket(
            'main-read-main-web-bucket-viewer',
            self.main_web_bucket,
            self.main_read_group,
            BucketMembership.APPEND,
        )

        self.infra.add_member_to_bucket(
            'full-main-web-bucket-admin',
            self.main_web_bucket,
            self.full_group,
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

            # upload_group has ADMIN
            self.infra.add_member_to_bucket(
                f'main-upload-upload-group-{bname}-bucket-admin',
                bucket=main_upload_bucket,
                member=self.upload_group,
                membership=BucketMembership.MUTATE,
            )

            # full GROUP has ADMIN
            self.infra.add_member_to_bucket(
                f'full-{bname}-bucket-admin',
                bucket=main_upload_bucket,
                member=self.full_group,
                membership=BucketMembership.MUTATE,
            )

            self.infra.add_member_to_bucket(
                f'main-read-{bname}-bucket-viewer',
                bucket=main_upload_bucket,
                member=self.main_read_group,
                membership=BucketMembership.READ,
            )

            # access GROUP has VIEWER
            # (semi surprising tbh, but useful for reading uploaded metadata)
            self.infra.add_member_to_bucket(
                f'analysis-group-{bname}-bucket-viewer',
                bucket=main_upload_bucket,
                member=self.analysis_group,
                membership=BucketMembership.READ,
            )

    @cached_property
    def main_bucket(self):
        return self.infra.create_bucket(
            'main',
            lifecycle_rules=[self.infra.bucket_rule_undelete()],
            autoclass=self.dataset_config.autoclass,
        )

    @cached_property
    def main_tmp_bucket(self):
        return self.infra.create_bucket(
            'main-tmp',
            lifecycle_rules=[self.infra.bucket_rule_temporary()],
            versioning=False,
            autoclass=False,  # Gets cleared out automatically.
        )

    @cached_property
    def main_analysis_bucket(self):
        return self.infra.create_bucket(
            'main-analysis',
            lifecycle_rules=[self.infra.bucket_rule_undelete()],
            autoclass=self.dataset_config.autoclass,
        )

    @cached_property
    def main_web_bucket(self):
        return self.infra.create_bucket(
            'main-web',
            lifecycle_rules=[self.infra.bucket_rule_undelete()],
            autoclass=self.dataset_config.autoclass,
        )

    @cached_property
    def main_upload_buckets(self) -> dict[str, Any]:
        main_upload_undelete = self.infra.bucket_rule_undelete(days=30)
        main_upload_buckets = {
            'main-upload': self.infra.create_bucket(
                'main-upload',
                lifecycle_rules=[main_upload_undelete],
                autoclass=self.dataset_config.autoclass,
            ),
        }

        for additional_upload_bucket in self.dataset_config.additional_upload_buckets:
            main_upload_buckets[additional_upload_bucket] = self.infra.create_bucket(
                additional_upload_bucket,
                lifecycle_rules=[main_upload_undelete],
                unique=True,
                autoclass=self.dataset_config.autoclass,
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
            self.infra.add_member_to_bucket(
                f'test-full-{bucket_name}-admin',
                bucket,
                self.test_full_group,
                BucketMembership.MUTATE,
            )

            self.infra.add_member_to_bucket(
                f'test-read-{bucket_name}-read',
                bucket,
                self.test_read_group,
                BucketMembership.READ,
            )

        # give web-server access to test-bucket
        if (
            isinstance(self.infra, GcpInfrastructure)
            and self.config.web_service is not None
        ):
            self.infra.add_member_to_bucket(
                'web-server-test-web-bucket-viewer',
                bucket=self.test_web_bucket,
                member=self.config.web_service.gcp.server_machine_account,  # WEB_SERVER_SERVICE_ACCOUNT,
                membership=BucketMembership.READ,
            )

    @cached_property
    def test_bucket(self):
        return self.infra.create_bucket(
            'test',
            lifecycle_rules=[self.infra.bucket_rule_undelete()],
            autoclass=self.dataset_config.autoclass,
        )

    @cached_property
    def test_analysis_bucket(self):
        return self.infra.create_bucket(
            'test-analysis',
            lifecycle_rules=[self.infra.bucket_rule_undelete()],
            autoclass=self.dataset_config.autoclass,
        )

    @cached_property
    def test_web_bucket(self):
        return self.infra.create_bucket(
            'test-web',
            lifecycle_rules=[self.infra.bucket_rule_undelete()],
            autoclass=self.dataset_config.autoclass,
        )

    @cached_property
    def test_tmp_bucket(self):
        return self.infra.create_bucket(
            'test-tmp',
            lifecycle_rules=[self.infra.bucket_rule_temporary()],
            versioning=False,
            autoclass=False,  # Gets cleared out automatically.
        )

    @cached_property
    def test_upload_bucket(self):
        return self.infra.create_bucket(
            'test-upload',
            lifecycle_rules=[self.infra.bucket_rule_undelete()],
            autoclass=self.dataset_config.autoclass,
        )

    # endregion TEST BUCKETS
    # region RELEASE BUCKETS

    def setup_storage_release_bucket_permissions(self):
        self.infra.add_member_to_bucket(
            'analysis-group-release-bucket-viewer',
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
            self.full_group,
            BucketMembership.MUTATE,
        )

    @cached_property
    def release_bucket(self):
        return self.infra.create_bucket(
            'release',
            lifecycle_rules=[self.infra.bucket_rule_undelete()],
            requester_pays=True,
            autoclass=self.dataset_config.autoclass,
        )

    # endregion RELEASE BUCKETS
    # endregion STORAGE
    # region HAIL

    def setup_hail(self):
        self.setup_hail_billing_project()
        self.setup_git_checkout_token_permissions()
        self.setup_hail_bucket_permissions()
        self.setup_hail_wheels_bucket_permissions()

    @cached_property
    def hail_batch_billing_project(self):
        assert self.config.hail

        if isinstance(self.infra, GcpInfrastructure):
            if not self.config.hail.gcp:
                raise ValueError('config.hail.gcp was not set to find hail_batch_url')
            hail_batch_url = self.config.hail.gcp.hail_batch_url
        elif isinstance(self.infra, AzureInfra):
            if not self.config.hail.azure:
                raise ValueError('config.hail.azure was not set to find hail_batch_url')
            hail_batch_url = self.config.hail.azure.hail_batch_url
        elif isinstance(self.infra, DryRunInfra):
            return None
        else:
            raise ValueError(
                f'Unknown infra type {type(self.infra)} for '
                'building hail_batch_billing_project',
            )

        return HailBatchBillingProject(
            self.infra.get_pulumi_name('batch-billing-project'),
            billing_project_name=self.dataset_config.dataset,
            batch_uri=hail_batch_url,
            token_category=self.infra.name(),
        )

    def setup_hail_billing_project(self):
        _ = self.hail_batch_billing_project

    def setup_git_checkout_token_permissions(self):
        if (
            isinstance(self.infra, GcpInfrastructure)
            and self.config.hail
            and self.config.hail.gcp.git_credentials_secret_name
        ):
            for name, access_group in self.access_level_groups.items():
                self.infra.add_secret_member(
                    f'git-checkout-token-{name}-accessor',
                    secret=self.config.hail.gcp.git_credentials_secret_name,
                    project=self.config.hail.gcp.git_credentials_secret_project,
                    member=access_group,
                    membership=SecretMembership.ACCESSOR,
                )

    def setup_hail_bucket_permissions(self):
        for (
            access_level,
            hail_machine_account,
        ) in self.hail_accounts_by_access_level.items():
            # Full access to the Hail Batch bucket.
            self.infra.add_member_to_bucket(
                f'hail-service-account-{access_level}-hail-bucket-admin',
                self.hail_bucket,
                hail_machine_account.cloud_id,
                BucketMembership.MUTATE,
            )

        if self.should_setup_analysis_runner:
            if isinstance(self.infra, GcpInfrastructure):
                # The analysis-runner needs Hail bucket access for compiled code.
                # ANALYSIS_RUNNER_SERVICE_ACCOUNT
                self.infra.add_member_to_bucket(
                    'analysis-runner-hail-bucket-admin',
                    bucket=self.hail_bucket,
                    member=self.config.analysis_runner.gcp.server_machine_account,
                    membership=BucketMembership.MUTATE,
                )

    def setup_hail_wheels_bucket_permissions(self):
        keys = {'analysis-group': self.analysis_group, **self.access_level_groups}

        bucket = None
        if isinstance(self.infra, GcpInfrastructure):
            assert self.config.hail
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
    def hail_accounts_by_access_level(self) -> dict[str, HailAccount]:
        if not self.should_setup_hail:
            return {}

        if isinstance(self.infra, GcpInfrastructure):
            accounts = {
                'standard': self.dataset_config.gcp.hail_service_account_standard,
                'full': self.dataset_config.gcp.hail_service_account_full,
            }
            if self.dataset_config.setup_test:
                accounts['test'] = self.dataset_config.gcp.hail_service_account_test
        elif isinstance(self.infra, AzureInfra):
            assert (
                self.dataset_config.azure is not None
            ), 'dataset_config.azure is required to be set'
            accounts = {
                'test': self.dataset_config.azure.hail_service_account_test,
                'standard': self.dataset_config.azure.hail_service_account_standard,
            }
            if self.dataset_config.setup_test:
                accounts['test'] = self.dataset_config.azure.hail_service_account_test
        else:
            return {}

        return {cat: ac for cat, ac in accounts.items() if ac}

    @cached_property
    def hail_bucket(self):
        return self.infra.create_bucket(
            'hail',
            lifecycle_rules=[self.infra.bucket_rule_temporary()],
            autoclass=False,  # Gets cleared out automatically.
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
            self.infra.add_member_to_machine_account_role(
                f'cromwell-service-account-{access_level}-service-account-user',
                machine_account,
                machine_account,
                role=MachineAccountRole.ACCESS,
            )

            # TODO: test if this is necessary, I don't think it should be :suss:
            # Allow the Cromwell SERVER to run worker VMs using the Cromwell SAs
            assert self.config.cromwell
            self.infra.add_member_to_machine_account_role(
                f'cromwell-runner-{access_level}-service-account-user',
                machine_account,
                self.config.cromwell.gcp.runner_machine_account,
                # admin access / credentials generator is the same thing
                role=MachineAccountRole.ACCESS,
            )

        if isinstance(self.infra, GcpInfrastructure):
            self._gcp_setup_cromwell()

    def setup_cromwell_credentials(self):
        assert self.config.analysis_runner
        for (
            access_level,
            cromwell_account,
        ) in self.cromwell_machine_accounts_by_access_level.items():
            secret = self.infra.create_secret(
                f'{self.dataset_config.dataset}-cromwell-{access_level}-key',
                project=self.config.analysis_runner.gcp.project,  # ANALYSIS_RUNNER_PROJECT,
            )

            credentials = self.infra.get_credentials_for_machine_account(
                f'cromwell-service-account-{access_level}-key',
                cromwell_account,
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
                if hail_account := self.hail_accounts_by_access_level.get(access_level):
                    self.infra.add_secret_member(
                        f'cromwell-service-account-{access_level}-self-accessor',
                        project=self.config.analysis_runner.gcp.project,  # ANALYSIS_RUNNER_PROJECT,
                        secret=secret,
                        member=hail_account.cloud_id,
                        membership=SecretMembership.ACCESSOR,
                    )

    @cached_property
    def cromwell_machine_accounts_by_access_level(self) -> dict[AccessLevel, Any]:
        if not self.should_setup_cromwell:
            return {}

        return {
            access_level: self.infra.create_machine_account(f'cromwell-{access_level}')
            for access_level in access_levels(
                include_test=self.dataset_config.setup_test,
            )
        }

    def _gcp_setup_cromwell(self) -> None:
        assert isinstance(self.infra, GcpInfrastructure)
        assert self.config.cromwell

        # Add Hail service accounts to (premade) Cromwell access group.
        for access_level, hail_account in self.hail_accounts_by_access_level.items():
            # premade google group, so don't manage this one
            self.infra.add_group_member(
                f'hail-service-account-{access_level}-cromwell-access',
                group=self.config.cromwell.gcp.access_group_id,  # CROMWELL_ACCESS_GROUP_ID,
                member=hail_account.cloud_id,
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
            self.infra.add_member_to_machine_account_role(
                f'hail-service-account-{access_level}-dataproc-service-account-user',
                spark_accounts[access_level],
                hail_account.cloud_id,
                role=MachineAccountRole.ACCESS,
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
                    account=hail_account.cloud_id,
                    role='admin',
                )

                # Give hail worker permissions to submit jobs.
                self.infra.add_member_to_dataproc_api(
                    f'hail-service-account-{access_level}-dataproc-worker',
                    account=hail_account.cloud_id,
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

        return {
            access_level: self.infra.create_machine_account(f'dataproc-{access_level}')
            for access_level in access_levels(
                include_test=self.dataset_config.setup_test,
            )
        }

    # endregion SPARK
    # region SAMPLE METADATA

    def setup_metamist(self):
        if not self.should_setup_metamist:
            return

        self.setup_metamist_access_permissions()

        if isinstance(self.infra, GcpInfrastructure):
            # do some cloudrun stuff
            self.setup_metamist_cloudrun_permissions()
            # setup list access for metamist to dataset bucket objects
            self.setup_metamist_dataset_storage_permissions()
        elif isinstance(self.infra, AzureInfra):
            # we'll do some custom stuff here :)
            raise NotImplementedError

    @cached_property
    def metamist_groups(
        self,
    ) -> dict[str, CPGInfrastructure.GroupProvider.Group]:
        if not self.should_setup_metamist:
            return {}

        return {
            key: self.create_group(f'sample-metadata-{key}', cache_members=True)
            for key in METAMIST_PERMISSIONS
        }

    def setup_metamist_dataset_storage_permissions(self):
        assert isinstance(self.infra, GcpInfrastructure)

        # add metamist machine account to the `main-list` group for the dataset.
        # this group gives list access to the dataset buckets but grants no ability
        # to read the actual contents of objects
        self.main_list_group.add_member(
            self.infra.get_pulumi_name('metamist-service-account-in-main-list'),
            self.infra.config.metamist.gcp.machine_account,
        )

    def setup_metamist_cloudrun_permissions(self):
        # now we give the metamist_access_group access to cloud-run instance
        assert isinstance(self.infra, GcpInfrastructure)

        self.root.gcp_metamist_invoker_group.add_member(
            self.infra.get_pulumi_name('sample-metadata-analysis-invoker'),
            self.analysis_group,
        )
        for sm_type, group in self.metamist_groups.items():
            self.root.gcp_metamist_invoker_group.add_member(
                self.infra.get_pulumi_name(f'sample-metadata-{sm_type}-invoker'),
                group,
            )

    def setup_metamist_access_permissions(self):
        if not self.should_setup_metamist:
            return

        if self.config.billing and self.config.billing.coordinator_machine_account:
            # make sure billing_coordinator can access sample metadata
            self.metamist_groups[SM_MAIN_READ].add_member(
                self.infra.get_pulumi_name(
                    'sample-metadata-main-read-billing-coordinator',
                ),
                self.config.billing.coordinator_machine_account,
            )

        sm_access_levels: list[SampleMetadataAccessorMembership] = [
            SampleMetadataAccessorMembership(
                name='human',
                member=self.analysis_group,
                permissions=(SM_MAIN_READ, SM_TEST_READ, SM_TEST_WRITE),
            ),
            SampleMetadataAccessorMembership(
                name='data-manager-group',
                member=self.data_manager_group,
                permissions=METAMIST_PERMISSIONS,
            ),
            SampleMetadataAccessorMembership(
                name='metadata-group',
                member=self.metadata_access_group,
                permissions=(SM_MAIN_READ, SM_TEST_READ, SM_TEST_WRITE),
            ),
            SampleMetadataAccessorMembership(
                name='test-read',
                member=self.test_read_group,
                permissions=(SM_MAIN_READ, SM_TEST_READ, SM_TEST_WRITE),
            ),
            SampleMetadataAccessorMembership(
                name='test-write',
                member=self.test_full_group,
                permissions=(SM_MAIN_READ, SM_TEST_READ, SM_TEST_WRITE),
            ),
            SampleMetadataAccessorMembership(
                name='main-read',
                member=self.main_read_group,
                permissions=(SM_MAIN_READ,),
            ),
            SampleMetadataAccessorMembership(
                name='main-write',
                member=self.main_create_group,
                permissions=(SM_MAIN_READ, SM_MAIN_WRITE),
            ),
            SampleMetadataAccessorMembership(
                name='full',
                member=self.full_group,
                permissions=METAMIST_PERMISSIONS,
            ),
        ]

        if self.config.analysis_runner:
            # allow the analysis-runner logging cloud function to update the sample-metadata project
            sm_access_levels.append(
                SampleMetadataAccessorMembership(
                    name='analysis-runner-logger',
                    member=self.config.analysis_runner.gcp.logger_machine_account,
                    permissions=METAMIST_PERMISSIONS,
                ),
            )

        # extra custom SAs
        extra_sm_read_sas = self.dataset_config.sm_read_only_sas
        extra_sm_write_sas = self.dataset_config.sm_read_write_sas

        for sa in extra_sm_read_sas:
            sm_access_levels.append(
                SampleMetadataAccessorMembership(
                    name=self._get_name_from_external_sa(sa),
                    member=sa,
                    permissions=(SM_MAIN_READ,),
                ),
            )
        for sa in extra_sm_write_sas:
            sm_access_levels.append(
                SampleMetadataAccessorMembership(
                    name=self._get_name_from_external_sa(sa),
                    member=sa,
                    permissions=(SM_MAIN_READ, SM_MAIN_WRITE),
                ),
            )

        for name, member, permission in sm_access_levels:
            for kind in permission:
                self.metamist_groups[kind].add_member(
                    self.infra.get_pulumi_name(
                        f'sample-metadata-{kind}-{name}-group-membership',
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
        self.setup_container_read_write_permissions()
        self.setup_dataset_container_registry()
        self.setup_analysis_runner_container_registry()

    def setup_analysis_runner_container_registry(self):
        if (
            not isinstance(self.infra, GcpInfrastructure)
            or self.config.analysis_runner is None
        ):
            return

        if self.dataset_config.dataset != self.config.common_dataset:
            return

        assert self.config.analysis_runner

        self.infra.add_member_to_container_registry(
            'images-reader-in-analysis-runner',
            registry=self.config.analysis_runner.gcp.container_registry_name,
            project=self.config.analysis_runner.gcp.project,
            member=self.images_reader_group,
            membership=ContainerRegistryMembership.READER,
        )

    def setup_container_read_write_permissions(self):
        """
        Add members to images-reader/writer groups
        :return:
        """
        self.images_writer_group.add_member(
            self.infra.get_pulumi_name('standard-in-images-writer-group-member'),
            self.standard_group,
        )
        self.images_writer_group.add_member(
            self.infra.get_pulumi_name('full-in-images-writer-group-member'),
            self.full_group,
        )

        accounts = {'analysis': self.analysis_group, **self.access_level_groups}
        for kind, account in accounts.items():
            self.images_reader_group.add_member(
                self.infra.get_pulumi_name(f'{kind}-in-images-reader-group-member'),
                account,
            )

    def setup_dataset_container_registry(self):
        """
        If required, setup a container registry for a dataset
        :return:
        """
        if not self.dataset_config.create_container_registry:
            return

        # mostly because this current format requires the project_id
        main_container_registry = self.infra.create_container_registry('images')
        dev_container_registry = self.infra.create_container_registry('images-dev')

        self.infra.add_member_to_container_registry(
            'images-reader-in-container-registry',
            registry=main_container_registry,
            member=self.images_reader_group,
            membership=ContainerRegistryMembership.READER,
        )
        self.infra.add_member_to_container_registry(
            'images-writer-in-container-registry',
            registry=main_container_registry,
            member=self.images_writer_group,
            membership=ContainerRegistryMembership.WRITER,
        )
        self.infra.add_member_to_container_registry(
            'test-full-reader-in-dev-container-registry',
            registry=dev_container_registry,
            member=self.test_full_group,
            membership=ContainerRegistryMembership.READER,
        )
        self.infra.add_member_to_container_registry(
            'analysis-writer-in-dev-container-registry',
            registry=dev_container_registry,
            member=self.analysis_group,
            membership=ContainerRegistryMembership.WRITER,
        )

    # endregion CONTAINER REGISTRY
    # region NOTEBOOKS

    def setup_notebooks(self):
        self.setup_notebooks_account_permissions()

    def setup_notebooks_account_permissions(self):
        # allow access group to use notebook account
        self.infra.add_member_to_machine_account_role(
            'notebook-account-users',
            machine_account=self.notebook_account,
            member=self.analysis_group,
            role=MachineAccountRole.ACCESS,
        )

        # Grant the notebook account the same permissions as the access group members.
        self.analysis_group.add_member(
            self.infra.get_pulumi_name('notebook-service-account-group-member'),
            member=self.notebook_account,
        )

        if isinstance(self.infra, GcpInfrastructure):
            assert self.config.notebooks

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
                f'No implementation for compute.admin for notebook account on {self.infra.name()}',
            )

    @cached_property
    def notebook_account(self):
        assert self.config.notebooks
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
        assert self.config.analysis_runner
        self.infra.add_cloudrun_invoker(
            'analysis-runner-analysis-invoker',
            project=self.config.analysis_runner.gcp.project,  # ANALYSIS_RUNNER_PROJECT,
            service=self.config.analysis_runner.gcp.cloud_run_instance_name,  # ANALYSIS_RUNNER_CLOUD_RUN_INSTANCE_NAME,
            member=self.analysis_group,
        )

    def setup_analysis_runner_config_access(self):
        keys = {'analysis-group': self.analysis_group, **self.access_level_groups}

        for key, group in keys.items():
            if isinstance(self.infra, GcpInfrastructure):
                assert self.config.gcp
                bucket = self.config.gcp.config_bucket_name
            elif isinstance(self.infra, AzureInfra):
                assert self.config.azure
                bucket = self.config.azure.config_bucket_name
            else:
                raise ValueError(
                    f'Bucket could not be determined for {self.infra.name()}',
                )

            self.infra.add_member_to_bucket(
                f'{key}-analysis-runner-config-viewer',
                bucket=bucket,  # ANALYSIS_RUNNER_CONFIG_BUCKET_NAME,
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
                'Requested shared project, but no bucket is available to share.',
            )
        budget = self.dataset_config.budgets.get(self.infra.name())
        if not budget:
            raise ValueError(
                f'No budget was available for {self.dataset_config.dataset}.{self.infra.name()}',
            )
        if not budget.shared_total_budget:
            raise ValueError(
                'Requested shared project, but the dataset configuration option '
                f'"{self.dataset_config.dataset}.budgets.{self.infra.name()}'
                '.shared_total_budget" was not specified.',
            )

        shared_buckets = {'release': self.release_bucket}

        project_name = pulumi.Output.concat(self.infra.project_id, '-shared')

        shared_project = self.infra.create_project('shared-project', name=project_name)
        self.infra.create_fixed_budget(
            'shared-budget',
            project=shared_project,
            budget=budget.shared_total_budget,
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

            self.infra.add_member_to_machine_account_role(
                'shared-project-sa-data-manager-credentials-generator',
                machine_account=shared_ma,
                member=self.data_manager_group,
                role=MachineAccountRole.CREDENTIALS_ADMIN,
            )

            self.infra.add_project_role(
                'data-manager-shared-iam-viewer',
                member=self.data_manager_group,
                role='roles/iam.roleViewer',
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

    # region DEPENDENCIES

    def setup_dependencies(self):
        self.setup_dependencies_group_memberships()

    def setup_dependencies_group_memberships(self):
        # duplicate reference to avoid mutating config
        dependencies = list(self.dataset_config.depends_on)

        if (
            self.config.common_dataset
            and self.dataset_config.dataset != self.config.common_dataset
        ):
            dependencies.append(self.config.common_dataset)

        for dependency in dependencies:
            # Adding dependent groups in two ways for reference:

            transitive_groups = [
                self.analysis_group,
                self.data_manager_group,
                self.web_access_group,
                self.metadata_access_group,
                self.upload_group,
                self.main_list_group,
                self.main_read_group,
                self.main_create_group,
                self.full_group,
                self.images_reader_group,
                self.images_writer_group,
            ]

            if self.dataset_config.setup_test:
                transitive_groups.extend(
                    (
                        self.test_read_group,
                        self.test_read_group,
                        self.test_full_group,
                    ),
                )

            for group in transitive_groups:
                group_name = group.name.removeprefix(self.dataset_config.dataset + '-')
                transitive_group = self.group_provider.get_group(
                    self.infra.name(),
                    f'{dependency}-{group_name}',
                )
                transitive_group.add_member(
                    self.infra.get_pulumi_name(
                        f'transitive-{group_name}-in-{dependency}-{group_name}',
                    ),
                    group,
                )

        for dependency in self.dataset_config.depends_on_readonly:
            group_map = {
                'main-read': [
                    self.main_read_group,
                    self.main_create_group,
                    self.full_group,
                ],
                'main-list': [self.main_list_group],
                'images-reader': [self.images_reader_group],
            }
            if self.dataset_config.setup_test:
                group_map['test-read'] = [
                    self.test_read_group,
                    self.test_full_group,
                ]
            for target_group, groups in group_map.items():
                transitive_group = self.group_provider.get_group(
                    self.infra.name(),
                    f'{dependency}-{target_group}',
                )
                for group in groups:
                    transitive_group.add_member(
                        self.infra.get_pulumi_name(
                            f'transitive-{group.name}-in-{dependency}-{target_group}',
                        ),
                        group,
                    )

    # endregion DEPENDENCIES

    # region UTILS

    @staticmethod
    def _get_name_from_external_sa(
        email: str,
        suffix: str = '.iam.gserviceaccount.com',
    ) -> str:
        """
        Convert service account email to name + some filtering.

        >>> CPGDatasetCloudInfrastructure._get_name_from_external_sa('my-service-account@project.iam.gserviceaccount.com')
        'my-service-account-project'

        >>> CPGDatasetCloudInfrastructure._get_name_from_external_sa('yourname@populationgenomics.org.au')
        'yourname'

        >>> CPGDatasetCloudInfrastructure._get_name_from_external_sa('my.service-account+extra@domain.com')
        'my-service-account-extra'
        """
        base = email[: -len(suffix)] if email.endswith(suffix) else email.split('@')[0]

        return NON_NAME_REGEX.sub('-', base).replace('--', '-')

    # endregion UTILS


def test():
    infra_config_dict = dict(cpg_utils.config.get_config(print_config=False))
    infra_config_dict['infrastructure']['reference_dataset'] = 'fewgenomes'
    infra_config = CPGInfrastructureConfig.from_dict(infra_config_dict)

    configs = [
        CPGDatasetConfig(
            dataset='fewgenomes',
            deploy_locations=['dry-run'],
            gcp=CPGDatasetConfig.Gcp(
                project='test-project',
                hail_service_account_test=HailAccount(
                    cloud_id='fewgenomes-test@service-account',
                    username='fewgenomes-test',
                ),
                hail_service_account_standard=HailAccount(
                    cloud_id='fewgenomes-standard@service-account',
                    username='fewgenomes-standard',
                ),
                hail_service_account_full=HailAccount(
                    cloud_id='fewgenomes-full@service-account',
                    username='fewgenomes-full',
                ),
            ),
            budgets={'dry-run': CPGDatasetConfig.Budget(monthly_budget=100)},
        ),
    ]
    infra = CPGInfrastructure(infra_config, configs)
    infra.main()


if __name__ == '__main__':
    test()
