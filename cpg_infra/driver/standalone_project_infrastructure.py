# flake8: noqa: PGH003,ANN204,C901,ERA001,ANN401,SIM102
"""
CPGStandaloneProjectInfrastructure - infrastructure for a single project on GCP.
"""

from __future__ import annotations

from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.config import (
    CPGInfrastructureConfig,
    CPGStandaloneProjectConfig,
    infra_context_from_standalone_config,
)


class CPGStandaloneProjectInfrastructure:
    def __init__(
        self,
        config: CPGInfrastructureConfig,
        project_config: CPGStandaloneProjectConfig,
    ) -> None:
        self.config = config
        self.project_config = project_config

        self.infra = GcpInfrastructure(
            config=config,
            context=infra_context_from_standalone_config(project_config),
        )

    def main(self):
        # GcpInfrastructure implicitly creates a GCP project on the first access of its
        # `project_id` or `project` property, where the created resource is cached
        # for all subsequent accesses.
        self.infra.create_monthly_budget(
            resource_key='budget',
            project=self.infra.project,
            budget=self.project_config.monthly_budget,
        )

        owner_user = self.config.users.get(self.project_config.owner)
        if not owner_user:
            raise ValueError(f'Owner {self.project_config.owner} not found in config')

        gcp_cloud = owner_user.clouds.get('gcp')
        if not gcp_cloud:
            raise ValueError(f'Owner {self.project_config.owner} has no gcp cloud id')

        self.infra.add_project_role(
            resource_key='project-owner',
            project=self.infra.project_id,
            member=gcp_cloud.id,
            role='roles/owner',
        )
