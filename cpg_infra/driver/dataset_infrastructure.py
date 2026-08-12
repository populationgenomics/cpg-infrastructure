# flake8: noqa: F401,PGH003,ANN204
"""
CPGDatasetInfrastructure - infrastructure for a single dataset across
one or more cloud providers.
"""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Type

# Side-effect imports: bringing each concrete ``CloudInfraBase`` subclass into
# the process registers it with ``CloudInfraBase.__subclasses__()`` so
# ``NAME_TO_INFRA_CLASS`` below can pick it up.
#
# **Add a new CloudInfraBase subclass here.** If you add a new cloud provider
# (e.g. Seqera / Nextflow / whatever comes next) and forget to import it here,
# ``NAME_TO_INFRA_CLASS`` will silently omit it and this class will fail to
# instantiate the deploy location.
from cpg_infra.abstraction.azure import AzureInfra
from cpg_infra.abstraction.base import CloudInfraBase, DryRunInfra
from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.abstraction.metamist import MetamistProject
from cpg_infra.config import (
    CloudName,
    CPGDatasetConfig,
    CPGInfrastructureConfig,
)
from cpg_infra.driver.dataset_cloud_infrastructure import (
    CPGDatasetCloudInfrastructure,
)
from cpg_infra.driver.group_provider import GroupProvider

if TYPE_CHECKING:
    from cpg_infra.driver.infrastructure import CPGInfrastructure


NAME_TO_INFRA_CLASS: dict[str, Type[CloudInfraBase]] = {
    c.name(): c  # type: ignore
    for c in CloudInfraBase.__subclasses__()
}


class CPGDatasetInfrastructure:
    """
    Logic for building infrastructure for a single dataset
    for one infrastructure object.
    """

    def __init__(
        self,
        config: CPGInfrastructureConfig,
        root: CPGInfrastructure,
        group_provider: GroupProvider,
        dataset_config: CPGDatasetConfig,
    ) -> None:
        self.config = config
        self.root = root
        self.group_provider = group_provider

        self.dataset: str = dataset_config.dataset
        self.dataset_config: CPGDatasetConfig = dataset_config
        self.deploy_locations = dataset_config.deploy_locations

        self.clouds: dict[CloudName, CPGDatasetCloudInfrastructure] = {
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
            meta=self.dataset_config.metamist_project_meta(),
        )

    @cached_property
    def metamist_test_project(self):
        return MetamistProject(
            f'metamist-project-{self.dataset}-test',
            project_name=self.dataset + '-test',
            meta=self.dataset_config.metamist_project_meta(' (test)'),
        )
