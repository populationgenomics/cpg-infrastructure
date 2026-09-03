# flake8: noqa: PGH003,ANN204,C901,ERA001,ANN401,SIM102
"""
CPGStandaloneProjectInfrastructure - infrastructure for a single project on GCP.
"""

from __future__ import annotations

from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.config import (
    CPGDatasetConfig,
    CPGInfrastructureConfig,
    CPGStandaloneProjectConfig,
)


class CPGStandaloneProjectInfrastructure:
    def __init__(
        self,
        config: CPGInfrastructureConfig,
        project_config: CPGStandaloneProjectConfig,
    ) -> None:
        self.config = config
        self.project_config = project_config

        # GcpInfrastructure has useful functions for creating and managing GCP projects
        # but it requires a CPGDatasetConfig.
        # We opt to create a synthetic CPGDatasetConfig with the minimum required fields.
        synthetic_dataset_config = CPGDatasetConfig(
            dataset=project_config.project_id,
            gcp=CPGDatasetConfig.Gcp(project=project_config.project_id),
            budgets={
                'gcp': CPGDatasetConfig.Budget(
                    monthly_budget=project_config.monthly_budget,
                ),
            },
        )

        self.infra = GcpInfrastructure(
            config=config,
            dataset_config=synthetic_dataset_config,
        )

    def main(self):
        # GcpInfrastructure implicitly creates a GCP project on the first access of its
        # `project_id` or `project` property, where the created resource is cached
        # for all subsequent accesses.
        self.infra.create_monthly_budget(
            resource_key='budget', 
            project=self.infra.project_id,
            budget=self.project_config.monthly_budget
        )

        self.infra.add_project_role(
            resource_key='project-owner',
            project=self.infra.project_id,
            member=self.project_config.owner,
            role='roles/owner'
        )