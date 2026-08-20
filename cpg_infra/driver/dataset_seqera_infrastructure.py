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

import pulumi
import pulumi_gcp as gcp

from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.config import SeqeraAccount, SeqeraWorkspacePair, SeqeraWorkspaceRef
from cpg_infra.driver.dynamic_providers.seqera import (
    GoogleBatchConfig,
    SeqeraComputeEnv,
    SeqeraGoogleCredentials,
    SeqeraWorkspace,
)

if TYPE_CHECKING:
    from cpg_infra.driver.dataset_cloud_infrastructure import (
        CPGDatasetCloudInfrastructure,
    )

_ACCESS_LEVEL_FULL = 'full'
_ACCESS_LEVEL_STANDARD = 'standard'
_ACCESS_LEVEL_TEST = 'test'
_MAIN_WORKSPACE_LEVELS = frozenset({_ACCESS_LEVEL_FULL, _ACCESS_LEVEL_STANDARD})

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
        levels = list(_MAIN_WORKSPACE_LEVELS)
        if self._dataset_config.setup_test:
            levels.append(_ACCESS_LEVEL_TEST)
        return levels

    @cached_property
    def _workspace_pair(self) -> SeqeraWorkspacePair:
        """workspace pair of this dataset's team."""

        assert self._config.seqera is not None
        assert self._dataset_config.team_ownership is not None
        return self._config.seqera.teams[self._dataset_config.team_ownership].workspaces

    def _workspace_ref_for_access_level(self, level: str) -> SeqeraWorkspaceRef | None:
        """Returns workspace config reference for access level."""

        if level in _MAIN_WORKSPACE_LEVELS:
            return self._workspace_pair.main
        return self._workspace_pair.test

    def _workspace_resource_for_access_level(
        self,
        level: str,
    ) -> SeqeraWorkspace | None:
        """Returns Pulumi resource."""

        if self._workspace_ref_for_access_level(level) is None:
            return None
        assert self._dataset_config.team_ownership is not None
        ws_type = 'main' if level in _MAIN_WORKSPACE_LEVELS else 'test'
        return self._parent.root.seqera_workspaces.get(
            (self._dataset_config.team_ownership, ws_type),
        )

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
        self._setup_credentials_and_compute_envs()

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
        # Ensure the provider is materialised so pool exists before bindings.
        _ = self._wif_provider

        for level, sa in self._service_accounts.items():
            workspace_id = self._workspace_ref_for_access_level(level).workspace_id

            if workspace_id is None:
                continue

            wif_subject = f'org:{org_id}:wsp:{workspace_id}:workflow'
            principal = self._wif_pool.name.apply(
                lambda pool_name, subject=wif_subject: (
                    f'principal://iam.googleapis.com/{pool_name}/subject/{subject}'
                ),
            )
            gcp.serviceaccount.IAMMember(
                self._infra.get_pulumi_name(f'seqera-{level}-wif-user'),
                service_account_id=sa.name,
                role='roles/iam.workloadIdentityUser',
                member=principal,
            )

    def _work_dir_for_access_level(self, level: str) -> pulumi.Output[str]:
        """The work dir for a compute env for this access level.
        corresponds to seqera work directory where pipelines store scratch data
        """
        if level == _ACCESS_LEVEL_TEST:
            bucket = (
                self._parent.test_tmp_bucket
            )  # TODO is it okay to use this bucket ?
        else:
            bucket = (
                self._parent.main_tmp_bucket
            )  # TODO is it okay to use this bucket ?
        base = self._infra.bucket_output_path(bucket)
        return pulumi.Output.concat(base, '/seqera/', level)

    def _setup_credentials_and_compute_envs(self) -> None:
        """Create credentials + GCP Batch compute env per access level in the relevant workspace."""
        assert isinstance(self._infra, GcpInfrastructure)
        infra = self._infra

        project_id: pulumi.Output[str] = infra.project_id

        for level in self._access_levels:
            workspace_resource = self._workspace_resource_for_access_level(
                level,
            )
            if workspace_resource is None:
                continue

            sa = self._service_accounts[level]
            dataset = self._dataset_config.dataset

            wif_credentials = SeqeraGoogleCredentials(
                self._infra.get_pulumi_name(f'seqera-cred-{dataset}-{level}'),
                workspace_id=workspace_resource.workspace_id,
                cred_name=f'{dataset}-{level}-cred',
                workload_identity_provider=self._wif_provider.name,
                service_account_email=sa.email,
                opts=pulumi.ResourceOptions(depends_on=[workspace_resource]),
            )

            SeqeraComputeEnv(
                self._infra.get_pulumi_name(f'seqera-ce-{dataset}-{level}'),
                workspace_id=workspace_resource.workspace_id,
                ce_name=f'{dataset}-{level}',
                credentials_id=wif_credentials.credentials_id,
                platform='google-batch',
                description=f'{dataset} {level} compute environment ',
                config=GoogleBatchConfig(
                    location=infra.region,
                    work_dir=self._work_dir_for_access_level(level),
                    service_account=sa.email,  # TODO have attached the same SA for runtime. Attach a different SA for runtime
                    project_id=project_id,
                    head_job_cpus=2,
                    head_job_memory_mb=4096,
                    compute_jobs_machine_type=[
                        'e2-small',
                        'e2-medium',
                        'e2-standard-2',
                    ],
                    spot=True,  # TODO compute values fixed for now, need to update this
                ),
                opts=pulumi.ResourceOptions(depends_on=[workspace_resource]),
            )
