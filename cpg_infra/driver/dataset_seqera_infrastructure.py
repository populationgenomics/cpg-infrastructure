"""Per-dataset Seqera Platform integration.

Creates the GCP service accounts, Workload Identity Federation pool +
OIDC provider, and IAM bindings that let Seqera Cloud run Google Batch
jobs on the dataset's behalf. Workspaces themselves are created manually
in Seqera and referenced via CPGInfrastructureConfig.seqera.workspace_ids.

Gating (should_setup_seqera) lives on CPGDatasetCloudInfrastructure —
this class assumes it is only instantiated when it should run.
"""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING

import pulumi_gcp as gcp

from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.config import SeqeraAccount

if TYPE_CHECKING:
    from cpg_infra.driver.dataset_cloud_infrastructure import (
        CPGDatasetCloudInfrastructure,
    )

_ACCESS_LEVELS_ALWAYS = ('full', 'standard')
_ACCESS_LEVEL_TEST = 'test'

_SEQERA_SA_PROJECT_ROLES: tuple[str, ...] = (
    'roles/batch.jobsEditor',
    'roles/batch.agentReporter',
    'roles/logging.logWriter',
)


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
    def _workspace_id(self) -> int:
        # By the time this property is read, should_setup_seqera on the parent
        # has already validated that seqera config and team_ownership exist.
        assert self._config.seqera is not None
        assert self._dataset_config.team_ownership is not None
        return self._config.seqera.workspace_ids[self._dataset_config.team_ownership]

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
        """Materialise WIF pool, provider, SAs, and IAM bindings.

        Idempotent — cached_properties gate resource creation.
        """
        self._grant_project_roles()
        self._bind_wif_principals()

    def _grant_project_roles(self) -> None:
        for level, sa in self._service_accounts.items():
            for role in _SEQERA_SA_PROJECT_ROLES:
                role_slug = role.split('/')[-1].replace('.', '-')
                self._infra.add_project_role(
                    f'seqera-{level}-{role_slug}',
                    member=sa,
                    role=role,
                )

    def _bind_wif_principals(self) -> None:
        assert self._config.seqera is not None
        org_id = self._config.seqera.org_id
        workspace_id = self._workspace_id
        wif_subject = f'org:{org_id}:wsp:{workspace_id}:workflow'
        principal = self._wif_pool.name.apply(
            lambda pool_name: (
                f'principal://iam.googleapis.com/{pool_name}/subject/{wif_subject}'
            ),
        )
        # Ensure the provider is materialised so pool exists before bindings.
        _ = self._wif_provider

        for level, sa in self._service_accounts.items():
            gcp.serviceaccount.IAMMember(
                self._infra.get_pulumi_name(f'seqera-{level}-wif-user'),
                service_account_id=sa.name,
                role='roles/iam.workloadIdentityUser',
                member=principal,
            )
