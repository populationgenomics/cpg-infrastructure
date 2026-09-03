"""Per-dataset Seqera Platform integration.

Creates the GCP service accounts, Workload Identity Federation pool +
OIDC provider, and IAM bindings that let Seqera Cloud run Google Batch
jobs on the dataset's behalf. Workspaces themselves are created manually
in Seqera and referenced via CPGInfrastructureConfig.seqera.teams.

Two service accounts are created per access level:
  - Head Job SA (seqera-{level}-head): launches batch jobs; no dataset access.
  - Task Job SA (seqera-{level}): executes tasks with dataset access; no launch permission.

Gating (should_setup_seqera) lives on CPGDatasetCloudInfrastructure —
this class assumes it is only instantiated when it should run.
"""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING

import pulumi_gcp as gcp

from cpg_infra.abstraction.base import MachineAccountRole
from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.config import SeqeraAccount

if TYPE_CHECKING:
    import pulumi

    from cpg_infra.driver.dataset_cloud_infrastructure import (
        CPGDatasetCloudInfrastructure,
    )

_ACCESS_LEVELS_ALWAYS = ('full', 'standard')
_ACCESS_LEVEL_TEST = 'test'

# Head Job SA launches batch jobs; it does not need dataset access.
_HEAD_JOB_ROLES: tuple[str, ...] = (
    'roles/batch.jobsEditor',
    'roles/logging.viewer',
    'roles/iam.workloadIdentityUser',
)

# Task Job SA runs the actual compute tasks; no job-launch permission.
_TASK_JOB_ROLES: tuple[str, ...] = (
    'roles/batch.agentReporter',
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
    def _head_sas(self) -> dict[str, gcp.serviceaccount.Account]:
        """Head Job SAs per access level that launch batch jobs; no dataset access."""
        return {
            level: self._infra.create_machine_account(f'seqera-{level}-head')
            for level in self._access_levels
        }

    @cached_property
    def _wif_pool(self) -> gcp.iam.WorkloadIdentityPool:
        # display_name and pool_id share GCP's 32-char cap, so mirror them:
        # if the pool_id would fit, so does display_name. The Pulumi resource
        # name skips get_pulumi_name() to avoid the `{dataset}-gcp-` prefix
        # duplicating the dataset segment.
        pool_id = f'{self._dataset_config.dataset}-sqwif'
        return gcp.iam.WorkloadIdentityPool(
            pool_id,
            workload_identity_pool_id=pool_id,
            display_name=pool_id,
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
        """The Task Seqera SAs keyed by access level.

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
        """Materialise WIF pool, provider, SAs, and IAM bindings.

        Idempotent — cached_properties gate resource creation.
        """
        self._grant_project_roles()
        self._bind_wif_principals()

    def _grant_project_roles(self) -> None:
        # Grant HEAD SA roles per access level
        for level, head_sa in self._head_sas.items():
            for role in _HEAD_JOB_ROLES:
                role_slug = role.split('/')[-1].replace('.', '-')
                self._infra.add_project_role(
                    f'seqera-{level}-head-{role_slug}',
                    member=head_sa,
                    role=role,
                )
        # Grant TASK SA roles
        for level, sa in self._service_accounts.items():
            for role in _TASK_JOB_ROLES:
                role_slug = role.split('/')[-1].replace('.', '-')
                self._infra.add_project_role(
                    f'seqera-{level}-{role_slug}',
                    member=sa,
                    role=role,
                )
        # Grant HEAD SA roles/iam.serviceAccountUser on TASK SAs per access level
        for level, head_sa in self._head_sas.items():
            task_sa = self._service_accounts[level]
            self._infra.add_member_to_machine_account_role(
                f'seqera-{level}-head-sa-user',
                machine_account=task_sa,
                member=head_sa,
                role=MachineAccountRole.ACCESS,
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

        # Head SAs are bound to their respective workspace per access level.
        for level, head_sa in self._head_sas.items():
            ws_type = _WORKSPACE_TYPE_FOR_LEVEL[level]
            ws_id = workspace_ids[ws_type]
            gcp.serviceaccount.IAMMember(
                self._infra.get_pulumi_name(f'seqera-{level}-head-wif-user'),
                service_account_id=head_sa.name,
                role='roles/iam.workloadIdentityUser',
                member=_make_principal(ws_id),
            )
