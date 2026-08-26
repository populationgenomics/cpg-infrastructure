# flake8: noqa: PGH003,ANN204,C901,ERA001,ANN401,SIM102
"""
CPGInfrastructure - top-level driver for CPG multi-dataset infrastructure.
"""

from __future__ import annotations

import json
import os.path
from collections import defaultdict
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable

import graphlib
import pulumi
import pulumi_gcp as gcp

from cpg_infra.abstraction.azure import AzureInfra
from cpg_infra.abstraction.base import (
    BucketMembership,
    CloudInfraBase,
    SecretMembership,
)
from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.abstraction.hailbatch import HailBatchBillingProjectMembership
from cpg_infra.abstraction.metamist import MetamistProjectMembers
from cpg_infra.driver.constants import (
    SM_MAIN_CONTRIBUTE,
    SM_MAIN_READ,
    SM_MAIN_WRITE,
    SM_TEST_CONTRIBUTE,
    SM_TEST_READ,
    SM_TEST_WRITE,
    compute_hash,
    dict_to_toml,
)
from cpg_infra.driver.dataset_infrastructure import CPGDatasetInfrastructure
from cpg_infra.driver.dynamic_providers.seqera import SeqeraWorkspace
from cpg_infra.driver.dynamic_providers.seqera.util.api_util import SeqeraApiClient
from cpg_infra.driver.groups import GroupMember, GroupProvider
from cpg_infra.github_wif.driver import PAM_BROKER_SA_NAME
from cpg_infra.plugin import get_plugins

if TYPE_CHECKING:
    from cpg_infra.config import (
        CPGDatasetConfig,
        CPGInfrastructureConfig,
        MemberKey,
        TeamOwnership,
    )
    from cpg_infra.driver.dataset_cloud_infrastructure import (
        CPGDatasetCloudInfrastructure,
    )
    from cpg_infra.driver.groups import Group


def get_formatted_team_name(team: str) -> str:
    return team.lower().replace(' ', '-')


def get_formatted_ws_name(is_test: bool, team: str) -> str:
    ws_team = team.replace(' ', '-')
    return f'{ws_team}-Test' if is_test else ws_team


class CPGInfrastructure:
    """Class for managing all CPG infrastructure"""

    def __init__(
        self,
        config: CPGInfrastructureConfig,
        dataset_configs: list[CPGDatasetConfig],
    ) -> None:
        self.config = config
        self.dataset_configs: dict[str, CPGDatasetConfig] = {
            d.dataset: d for d in dataset_configs
        }

        self.group_provider = GroupProvider(
            group_prefix=self.config.group_prefix,
        )

        self.dataset_infrastructures: dict[
            str,
            CPGDatasetInfrastructure,
        ] = defaultdict()

        self.seqera_workspaces: dict[
            tuple[TeamOwnership, str],
            SeqeraWorkspace,
        ] = {}

        self.seqera_workspace_participants: set[
            tuple[TeamOwnership, str, MemberKey]
        ] = set()

    @cached_property
    def common_dataset(self) -> CPGDatasetInfrastructure:
        # ensure it's setup
        self.setup_datasets()
        return self.dataset_infrastructures[self.config.common_dataset]

    @cached_property
    def common_gcp_infra(self) -> GcpInfrastructure:
        return self.common_dataset.clouds[GcpInfrastructure.name()].infra  # type: ignore

    @cached_property
    def common_azure_infra(self) -> AzureInfra:
        return self.common_dataset.clouds[AzureInfra.name()].infra  # type: ignore

    @cached_property
    def internal_logs_access_group_gcp(self) -> Group:
        g = self.group_provider.create_group(
            self.common_gcp_infra,
            name='internal-logs-access',
            cache_members=True,
        )
        for user in self.config.users.values():
            if (
                user.can_access_internal_dataset_logs
                and 'gcp' in user.clouds
                and user.clouds['gcp'].id
            ):
                h = compute_hash('', user.id, 'gcp')
                g.add_member(
                    self.common_gcp_infra.get_pulumi_name(f'internal-logs-access-{h}'),
                    user.clouds[GcpInfrastructure.name()].id,
                    user=user.clouds[GcpInfrastructure.name()],
                )
        return g

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

        # Setup PAM broker infrastructure if PAM is configured
        self.setup_pam_broker()

        # Workspaces should be created/imported before
        # calling deploy_datasets() which setup Seqera/GCP infra per dataset
        if self.config.seqera is not None:
            # Initialize the Seqera API Client singleton
            SeqeraApiClient(
                server_url=self.config.seqera.api_url,
                token_secret_name=self.config.seqera.token_secret_name,
            )

            # Setup Seqera workspaces
            self.setup_seqera_workspaces()

        # Deploy all the assets required for each dataset. Groups, permissions
        # storage buckets, metamist and hail users etc.
        self.deploy_datasets()

        # Deploy managed adhoc assets that are not associated with datasets.
        self.deploy_adhoc()

        plugins = get_plugins()
        initialised_plugins = []
        for plugin_name in self.config.plugins_enabled:
            if plugin_name not in plugins:
                raise Exception(f'Plugin `{plugin_name}` is not installed')
            plugin = plugins[plugin_name](self, self.config)
            plugin.main()

            initialised_plugins.append(plugin)

        # Up to this point the groups have not actually been created, go through
        # the groups data structure and create the necessary groups in the correct
        # order so that group dependencies can be handled
        self.finalize_groups()

        for plugin in initialised_plugins:
            plugin.on_group_finalisation()

        self.setup_hail_batch_billing_project_members()

        # Add read and write level members to metamist projects
        self.update_metamist_members()

        # Generate data dropbox config from dataset upload configs
        self.generate_dropbox_config()

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

    def deploy_adhoc(self):
        infra = self.common_gcp_infra

        for group in self.config.adhoc_groups or []:
            cloud_group = self.group_provider.create_group(
                infra,
                name=group.name,
                description=group.description,
                cache_members=False,
            )
            for member_id in group.members:
                member = self.config.users.get(member_id)
                if not member:
                    raise ValueError(f'Member {member_id} not found in config')

                if cloud_user := member.clouds.get(GcpInfrastructure.name()):
                    pulumi_name = f'{group.name}-{compute_hash("", member_id, GcpInfrastructure.name())}'
                    cloud_group.add_member(
                        infra.get_pulumi_name(pulumi_name),
                        member=cloud_user.id,
                        user=cloud_user,
                    )

    def setup_hail_batch_billing_project_members(self):
        internal_users = [
            user
            for user in self.config.users.values()
            if user.can_access_internal_dataset_logs
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
                    cloud_names = [
                        user.clouds[cloud].hail_batch_username
                        for user in internal_users
                        if cloud in user.clouds
                        and user.clouds[cloud].hail_batch_username
                    ]
                    hail_batch_usernames.update(hbu for hbu in cloud_names if hbu)

                def _make_add_member_function(
                    _data_provider: CPGDatasetCloudInfrastructure,
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
                                h = compute_hash(
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
                                GroupMember,
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
                contribute_members=prepare_group_members(infra, SM_MAIN_CONTRIBUTE),
                write_members=prepare_group_members(infra, SM_MAIN_WRITE),
            )

            if infra.dataset_config.setup_test:
                MetamistProjectMembers(
                    f'{dataset}-metamist-test-members',
                    metamist_project_name=infra.metamist_test_project.project_name,
                    read_members=prepare_group_members(infra, SM_TEST_READ),
                    contribute_members=prepare_group_members(infra, SM_TEST_CONTRIBUTE),
                    write_members=prepare_group_members(infra, SM_TEST_WRITE),
                )

    def generate_dropbox_config(self):
        if not self.config.data_dropbox:
            return

        # Flat list of dropboxes
        all_dropboxes: list[dict[str, Any]] = []
        dropbox_ids_by_project: dict[str, set[str]] = defaultdict(set)

        for project, config in self.dataset_configs.items():
            if not config.upload_config or not config.upload_config.dropboxes:
                continue

            dropboxes = config.upload_config.dropboxes
            additional_buckets = config.upload_config.additional_buckets

            # Generate list of upload bucket names, so that we can check the
            # `move_to_bucket` setting is valid.
            upload_bucket_names = [ad.name for ad in additional_buckets or []]
            # Allow moving to the default upload bucket
            upload_bucket_names.append('default')

            for dropbox in dropboxes:
                if (
                    dropbox.move_to_bucket
                    and dropbox.move_to_bucket not in upload_bucket_names
                ):
                    raise ValueError(
                        f'Dropbox move_to_bucket setting of {dropbox.move_to_bucket} refers to an unknown bucket'
                    )

                # Ensure that this dropbox isn't duplicated in the project
                if dropbox.id in dropbox_ids_by_project[project]:
                    raise ValueError(
                        f'Dropbox id {dropbox.id} is duplicated in project {project}'
                    )

                dropbox_ids_by_project[project].add(dropbox.id)

                dropbox_uploaders: list[str] = []

                # Go through uploaders, check their validity and get their
                # resolved user ids
                for uploader in dropbox.uploaders:
                    member = self.config.users.get(uploader)

                    if not member:
                        raise ValueError(f'Member {uploader} not found in config')

                    cloud_user = member.clouds.get('gcp')

                    if not cloud_user:
                        raise ValueError(
                            f'Member {uploader} does not have a gcp id specified'
                        )
                    user_id = cloud_user.id
                    dropbox_uploaders.append(user_id)

                all_dropboxes.append(
                    dropbox.model_dump()
                    | {'project': project, 'uploaders': dropbox_uploaders}
                )

        secret_name = 'dropbox-server-config'  # noqa: S105 # linter thinks this is a password but it ain't
        secret = self.common_gcp_infra.create_secret(
            name=secret_name, project=self.config.data_dropbox.gcp.project
        )

        # Write the dropbox config to the secret
        self.common_gcp_infra.add_secret_version(
            'data-dropbox-server-config-latest',
            secret=secret,
            contents=json.dumps({'dropboxes': all_dropboxes}),
        )

        self.common_gcp_infra.add_secret_member(
            'data-dropbox-server-config-accessor',
            secret=secret,
            project=self.config.data_dropbox.gcp.project,
            member=self.config.data_dropbox.gcp.server_machine_account,
            membership=SecretMembership.ACCESSOR,
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
                output['git_credentials_secret_name'] = (
                    self.config.hail.gcp.git_credentials_secret_name
                )
            if self.config.hail.gcp.git_credentials_secret_project is not None:
                output['git_credentials_secret_project'] = (
                    self.config.hail.gcp.git_credentials_secret_project
                )

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

    def setup_seqera_workspaces(self):
        """Import Seqera workspaces to pulumi state - created manually"""

        seqera_cfg = self.config.seqera
        assert seqera_cfg is not None

        for team_ownership, ws_pair in seqera_cfg.teams.items():
            formatted_team_name = get_formatted_team_name(team_ownership)
            for workspace_type, ws_configs in (
                ('main', ws_pair.main),
                ('test', ws_pair.test),
            ):
                is_test = workspace_type == 'test'
                self.seqera_workspaces[(team_ownership, workspace_type)] = (
                    SeqeraWorkspace(
                        f'seqera-ws-{formatted_team_name}-{workspace_type}',
                        org_id=seqera_cfg.org_id,
                        workspace_id=ws_configs.workspace_id,
                        ws_name=get_formatted_ws_name(is_test, team_ownership),
                        full_name=f'CPG {team_ownership}{" Test" if is_test else ""} Workspace',
                        visibility='PRIVATE',
                        description=ws_configs.description,
                    )
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
                ('sample-metadata', self.config.metamist.gcp.legacy_machine_account),
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
    def config_viewer_group(self):
        grp = self.group_provider.create_group(
            self.common_gcp_infra,
            cache_members=False,
            name='analysis-runner-config-viewers-group',
        )

        if isinstance(self.common_gcp_infra, GcpInfrastructure):
            assert self.config.gcp
            bucket = self.config.gcp.config_bucket_name
        elif isinstance(self.common_gcp_infra, AzureInfra):
            assert self.config.azure
            bucket = self.config.azure.config_bucket_name
        else:
            raise ValueError(
                f'Bucket could not be determined for {self.infra.name()}',
            )

        # create on parent analysis-runner-config-viewer-group
        # and assign bucket READ permissions to it
        self.common_gcp_infra.add_member_to_bucket(
            'analysis-runner-config-viewers',
            bucket=bucket,  # ANALYSIS_RUNNER_CONFIG_BUCKET_NAME,
            member=grp,
            membership=BucketMembership.READ,
        )
        return grp

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

    def setup_pam_broker(self):
        """
        Setup PAM broker service account.

        The PAM broker is used by GitHub Actions to request PAM grants
        on behalf of notebook service accounts. It's created once in the
        common project and shared across all datasets with PAM enabled.

        Note: The WIF bindings and GitHub secrets are set up separately
        in the github_wif stack, which references this SA by name.
        """
        # Check if any dataset has PAM enabled
        pam_enabled_datasets = [
            dc
            for dc in self.dataset_configs.values()
            if dc.allow_notebook_tmp_main_read
        ]
        if not pam_enabled_datasets:
            pulumi.log.info(
                'No datasets have PAM enabled for notebooks, skipping broker setup'
            )
            return

        # Get common dataset's project for the broker SA
        common_config = next(
            (
                dc
                for dc in self.dataset_configs.values()
                if dc.dataset == self.config.common_dataset
            ),
            None,
        )
        if not common_config:
            raise ValueError(
                'PAM requires common_dataset to be configured',
            )
        broker_project_id = common_config.gcp.project

        # Create the broker service account
        self._pam_broker_sa = gcp.serviceaccount.Account(
            f'{broker_project_id}-{PAM_BROKER_SA_NAME}',
            account_id=PAM_BROKER_SA_NAME,
            display_name='PAM Broker for GitHub Actions',
            description='Service account used by GitHub Actions to request PAM grants',
            project=broker_project_id,
            create_ignore_already_exists=True,
        )

    @property
    def pam_broker_sa(self):
        """The PAM broker service account, or None if not configured."""
        return getattr(self, '_pam_broker_sa', None)

    @cached_property
    def datasets_needing_pam_entitlement(self) -> set[str]:
        """
        Datasets that need a PAM entitlement created, either because they have
        PAM directly configured, or because they are a dependency of a dataset
        that does.

        This allows dependent datasets to have entitlements created automatically
        without requiring allow_notebook_tmp_main_read on every dataset in the
        dependency chain.
        """
        needs_entitlement: set[str] = set()
        for dc in self.dataset_configs.values():
            if dc.allow_notebook_tmp_main_read or dc.members.get(
                'tmp-main-read-access',
            ):
                needs_entitlement.add(dc.dataset)
                for dep in [*dc.depends_on, *dc.depends_on_readonly]:
                    needs_entitlement.add(dep)
        return needs_entitlement

    def setup_python_registry(self):
        """
        Setup the python registry permissions in gcp-common
        """
        # force the creation
        _ = self.gcp_python_registry
