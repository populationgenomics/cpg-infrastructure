"""Per-dataset Seqera Platform integration.

Creates the GCP service accounts, Workload Identity Federation pool +
OIDC provider, and IAM bindings that let Seqera Cloud run Google Batch
jobs on the dataset's behalf. Workspaces themselves are created manually
in Seqera and referenced via CPGInfrastructureConfig.seqera.teams.

Two service accounts are created per access level:
  - Head Job SA (seqera-head): launches batch jobs; no dataset access.
  - Task Job SA (seqera-{level}): executes tasks with dataset access; no launch permission.

Gating (should_setup_seqera) lives on CPGDatasetCloudInfrastructure —
this class assumes it is only instantiated when it should run.
"""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

import pulumi
import pulumi_gcp as gcp

from cpg_infra.abstraction.base import BucketMembership
from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.config import (
    CPGInfrastructureConfig,
    MemberKey,
    SeqeraAccount,
    TeamOwnership,
)
from cpg_infra.driver.constants import get_formatted_team_name
from cpg_infra.driver.dynamic_providers.seqera import (
    GoogleBatchConfig,
    SeqeraComputeEnv,
    SeqeraWorkspace,
    SeqeraWorkspaceParticipant,
)
from cpg_infra.driver.dynamic_providers.seqera.inputs.compute_environment import (
    ConfigEnvVariable,
    GoogleWifCredentialConfig,
)

if TYPE_CHECKING:
    from cpg_infra.driver.dataset_cloud_infrastructure import (
        CPGDatasetCloudInfrastructure,
    )

_ACCESS_LEVELS_ALWAYS = ('full', 'standard')
_ACCESS_LEVEL_TEST = 'test'

# Head Job SA launches batch jobs; it does not need dataset access.
_HEAD_JOB_ROLES: tuple[str, ...] = (
    'roles/batch.jobsEditor',
    'roles/logging.viewer',
)

# Task Job SA runs the actual compute tasks.
_TASK_JOB_ROLES: tuple[str, ...] = (
    'roles/batch.agentReporter',
    'roles/batch.jobsEditor',
    'roles/logging.logWriter',
)

# Maps each access level to the workspace type that holds it.
_WORKSPACE_TYPE_FOR_LEVEL: dict[str, str] = {
    'full': 'main',
    'standard': 'main',
    'test': 'test',
}


class DatasetSeqeraInfrastructure:
    """Owns all Seqera-related resources for one dataset."""

    def __init__(self, parent: CPGDatasetCloudInfrastructure) -> None:
        assert isinstance(parent.infra, GcpInfrastructure), (
            'DatasetSeqeraInfrastructure requires GcpInfrastructure; '
            'gating lives on CPGDatasetCloudInfrastructure.should_setup_seqera.'
        )
        self._parent = parent
        self._config = parent.config
        self._dataset_config = parent.dataset_config
        self._infra: GcpInfrastructure = parent.infra

    @cached_property
    def _access_levels(self) -> list[str]:
        levels = list(_ACCESS_LEVELS_ALWAYS)
        if self._dataset_config.setup_test:
            levels.append(_ACCESS_LEVEL_TEST)
        return levels

    @cached_property
    def _workspace_ids(self) -> dict[str, int]:
        # Returns {'main': <id>, 'test': <id>} for the dataset's team.
        assert self._config.seqera is not None
        assert self._dataset_config.team_ownership is not None
        team_workspaces = self._config.seqera.teams[self._dataset_config.team_ownership]
        return {
            'main': team_workspaces.main.workspace_id,
            'test': team_workspaces.test.workspace_id,
        }

    @cached_property
    def _team_workspace_pair(self) -> CPGInfrastructureConfig.Seqera.TeamWorkspaces:
        """Workspace pair corresponding to the team which own this dataset."""

        assert self._config.seqera is not None
        assert self._dataset_config.team_ownership is not None
        return self._config.seqera.teams[self._dataset_config.team_ownership]

    def _workspace_for_access_level(
        self, level: str
    ) -> CPGInfrastructureConfig.Seqera.WorkspaceConfig:
        """Returns workspace config reference for access level."""

        if level in _ACCESS_LEVELS_ALWAYS:
            return self._team_workspace_pair.main
        return self._team_workspace_pair.test

    def _workspace_resource_for_access_level(
        self,
        level: str,
    ) -> SeqeraWorkspace | None:
        """Returns Pulumi resource."""
        if self._workspace_for_access_level(level):
            assert self._dataset_config.team_ownership is not None
            ws_type = 'main' if level in _ACCESS_LEVELS_ALWAYS else 'test'
            return self._parent.root.seqera_workspaces.get(
                (self._dataset_config.team_ownership, ws_type),
            )
        return None

    @cached_property
    def _head_sa(self) -> gcp.serviceaccount.Account:
        """Single Head Job SA that launches batch jobs; no dataset access."""
        return self._infra.create_machine_account('seqera-head')

    @cached_property
    def _wif_pool(self) -> gcp.iam.WorkloadIdentityPool:
        pool_id = f'seqera-{self._dataset_config.dataset}'
        return gcp.iam.WorkloadIdentityPool(
            self._infra.get_pulumi_name(
                f'seqera-wif-pool-{self._dataset_config.dataset}'
            ),
            workload_identity_pool_id=pool_id,
            display_name=f'Seqera WIF pool for {self._dataset_config.dataset}',
            description=(
                'Federates Seqera Cloud OIDC tokens to per-dataset GCP '
                'service accounts.'
            ),
            project=self._infra.project_id,
        )

    @cached_property
    def _wif_provider(self) -> gcp.iam.WorkloadIdentityPoolProvider:
        assert self._config.seqera is not None
        return gcp.iam.WorkloadIdentityPoolProvider(
            self._infra.get_pulumi_name(
                f'seqera-wif-provider-{self._dataset_config.dataset}',
            ),
            workload_identity_pool_id=self._wif_pool.workload_identity_pool_id,
            workload_identity_pool_provider_id=(
                f'seqera-{self._dataset_config.dataset}-oidc'
            ),
            display_name='Seqera Cloud OIDC',
            project=self._infra.project_id,
            oidc=gcp.iam.WorkloadIdentityPoolProviderOidcArgs(
                issuer_uri=self._config.seqera.wif_issuer_uri,
            ),
            attribute_mapping={'google.subject': 'assertion.sub'},
        )

    def _account_id_for(self, level: str) -> str:
        # SAs are project-scoped (each dataset has its own GCP project), so
        # the dataset name would be redundant in the account_id. Keeping it
        # short also stays within GCP's 30-char account_id limit for datasets
        # with long names.
        return f'seqera-{level}'

    @cached_property
    def _service_accounts(
        self,
    ) -> dict[str, gcp.serviceaccount.Account]:
        return {
            level: self._infra.create_machine_account(self._account_id_for(level))
            for level in self._access_levels
        }

    @cached_property
    def accounts_by_access_level(self) -> dict[str, SeqeraAccount]:
        """The three Seqera SAs keyed by access level.

        account_id is built from the same plain string passed to
        create_machine_account() rather than read back from
        sa.account_id, because pulumi_gcp's Account.account_id is a
        pulumi.Output[str] at runtime, and SeqeraAccount.account_id is
        typed as a plain str (unlike cloud_id, which explicitly allows
        a pulumi.Output).
        """
        return {
            level: SeqeraAccount(
                account_id=self._account_id_for(level),
                cloud_id=sa.email,
            )
            for level, sa in self._service_accounts.items()
        }

    def setup(self) -> None:
        """Materialise WIF pool, provider, SAs, and IAM bindings in the GCP end,
        create compute environments and add participants to workspaces
        Idempotent — cached_properties gate resource creation.
        """
        self._grant_project_roles()
        self._bind_wif_principals()
        self._grant_service_account_self_user()
        self._grant_work_bucket_access()
        self._setup_seqera_compute_environments()
        self._setup_workspace_participants()

    def _grant_project_roles(self) -> None:
        for role in _HEAD_JOB_ROLES:
            role_slug = role.split('/')[-1].replace('.', '-')
            self._infra.add_project_role(
                f'seqera-head-{role_slug}',
                member=self._head_sa,
                role=role,
            )
        for level, sa in self._service_accounts.items():
            for role in _TASK_JOB_ROLES:
                role_slug = role.split('/')[-1].replace('.', '-')
                self._infra.add_project_role(
                    f'seqera-{level}-{role_slug}',
                    member=sa,
                    role=role,
                )

    def _bind_wif_principals(self) -> None:
        assert self._config.seqera is not None
        org_id = self._config.seqera.org_id
        # Ensure the provider is materialised so pool exists before bindings.
        _ = self._wif_provider

        def _make_principal(workspace_id: int) -> pulumi.Output[str]:
            wif_subject = f'org:{org_id}:wsp:{workspace_id}:workflow'
            return self._wif_pool.name.apply(
                lambda pool_name: (
                    f'principal://iam.googleapis.com/{pool_name}/subject/{wif_subject}'
                ),
            )

        workspace_ids = self._workspace_ids

        # Head SA is bound to both workspaces so it can launch jobs in either.
        for ws_type, ws_id in workspace_ids.items():
            gcp.serviceaccount.IAMMember(
                self._infra.get_pulumi_name(f'seqera-head-wif-user-{ws_type}'),
                service_account_id=self._head_sa.name,
                role='roles/iam.workloadIdentityUser',
                member=_make_principal(ws_id),
            )

        # Task SAs are bound only to the workspace that matches their access level.
        for level, sa in self._service_accounts.items():
            ws_type = _WORKSPACE_TYPE_FOR_LEVEL[level]
            ws_id = workspace_ids[ws_type]
            gcp.serviceaccount.IAMMember(
                self._infra.get_pulumi_name(f'seqera-{level}-wif-user'),
                service_account_id=sa.name,
                role='roles/iam.workloadIdentityUser',
                member=_make_principal(ws_id),
            )

    def _grant_service_account_self_user(self) -> None:
        # Nextflow head job can spawn child jobs that uses the same SA
        for level, sa in self._service_accounts.items():
            gcp.serviceaccount.IAMMember(
                self._infra.get_pulumi_name(f'seqera-{level}-self-user'),
                service_account_id=sa.name,
                role='roles/iam.serviceAccountUser',
                member=sa.email.apply(lambda email: f'serviceAccount:{email}'),
            )

    def _work_bucket_for_access_level(self, level: str) -> Any:
        if level == _ACCESS_LEVEL_TEST:
            return self._parent.nf_test_work_bucket
        return self._parent.nf_main_work_bucket

    def _grant_work_bucket_access(self) -> None:
        for level, sa in self._service_accounts.items():
            self._infra.add_member_to_bucket(
                f'seqera-{level}-work-bucket-admin',
                self._work_bucket_for_access_level(level),
                sa,
                BucketMembership.MUTATE,
            )

    def _work_dir_for_access_level(self, level: str) -> pulumi.Output[str]:
        """
        The work dir of a Seqera compute env for this access level.
        This directory may contain
            Execution & Script Files - System files to set up and run tasks
            Logs & Status Trackers - Used to monitor running tasks while or to record its final state.
            Data Files
                Input files - Symlinks
                Output files - files produced by task scripts
            Cache files - to resume, relaunch runs

            https://docs.seqera.io/platform-cloud/launch/cache-resume
        """
        base = self._infra.bucket_output_path(self._work_bucket_for_access_level(level))
        return pulumi.Output.concat(base, '/', level)

    def _setup_seqera_compute_environments(self) -> None:
        """Create Seqera GCP Batch compute env per access level in the relevant workspace."""

        project_id = self._infra.project_id

        for level in self._access_levels:
            workspace_resource = self._workspace_resource_for_access_level(
                level,
            )
            if workspace_resource is None:
                continue

            dataset = self._dataset_config.dataset

            SeqeraComputeEnv(
                self._infra.get_pulumi_name(f'seqera-ce-{dataset}-{level}'),
                workspace_id=workspace_resource.workspace_id,
                ce_name=f'{dataset}-{level}',
                credentials=GoogleWifCredentialConfig(
                    workload_identity_provider=self._wif_provider.name,
                    service_account_email=self._head_sa.email,
                ),
                platform='google-batch',
                description=f'{dataset} {level} compute environment ',
                config=GoogleBatchConfig(
                    location=self._infra.region,
                    work_dir=self._work_dir_for_access_level(level),
                    service_account=self._service_accounts[level].email,
                    project_id=project_id,
                    compute_jobs_machine_type=[
                        'e2-small',
                        'e2-medium',
                        'e2-standard-2',
                    ],
                    environment=[
                        ConfigEnvVariable(
                            name='NXF_CLOUDINFO_ENABLED',
                            value='false',
                            head=True,
                            compute=True,
                        )  # disable Nextflow optimal vm selection logic. This is because it does not consider the actual resource availability
                    ],
                    spot=True,
                ),
                opts=pulumi.ResourceOptions(depends_on=[workspace_resource]),
            )

    def _setup_workspace_participants(self) -> None:
        """Add this dataset's analysis members to the team's main and test
        workspaces.
        """
        assert self._config.seqera is not None
        team = self._dataset_config.team_ownership
        assert team is not None

        seqera_cfg = self._config.seqera
        root = self._parent.root

        # reference to participants in workspaces
        # the same member can be in different datasets (under same or different teamOwnership)
        # but they will be added once to respective workspaces
        workspace_participants: set[tuple[TeamOwnership, str, MemberKey]] = (
            root.seqera_workspace_participants
        )
        gcp_key = GcpInfrastructure.name()
        team_name = get_formatted_team_name(team)

        members_to_add: list[MemberKey] = self._dataset_config.members.get(
            'analysis', []
        ) + self._dataset_config.members.get('data-manager', [])

        for member_key in sorted(members_to_add):
            user = self._config.users.get(member_key)
            if user is None:
                pulumi.warn(f'Could not find the member:{member_key} in CPG users.')
                continue
            cloud_user = user.clouds.get(gcp_key)
            if cloud_user is None or not cloud_user.id:
                pulumi.warn(f'Could not find member: {member_key} id.')
                continue
            if not cloud_user.has_seqera_account:
                continue
            email = cloud_user.id

            main_ws_member_key = (team, 'main', member_key)
            if main_ws_member_key not in workspace_participants:
                main_ws = root.seqera_workspaces[(team, 'main')]
                SeqeraWorkspaceParticipant(
                    f'wsp-{team_name}-main-{member_key}',
                    org_id=seqera_cfg.org_id,
                    workspace_id=main_ws.workspace_id,
                    email=email,
                    role='view',
                )
                workspace_participants.add(main_ws_member_key)

            test_ws_member_key = (team, 'test', member_key)
            if test_ws_member_key not in workspace_participants:
                test_ws = root.seqera_workspaces[(team, 'test')]
                SeqeraWorkspaceParticipant(
                    f'wsp-{team_name}-test-{member_key}',
                    org_id=seqera_cfg.org_id,
                    workspace_id=test_ws.workspace_id,
                    email=email,
                    role='admin',
                )
                workspace_participants.add(test_ws_member_key)
