"""Ability to extend CPG infrastructure with plugins"""

import importlib.metadata

from cpg_infra.config import CPGInfrastructureConfig

PLUGIN_ENTRYPOINT_NAME = 'cpginfra.plugins'


class CpgInfrastructurePlugin:
    """Billing aggregator Infrastructure (as code) for Pulumi"""

    def __init__(
        self,
        infrastructure,  # noqa: ANN001
        config: CPGInfrastructureConfig,
    ) -> None:
        self.config = config
        self.infrastructure = infrastructure

    def main(self):
        """Driver for the billing aggregator infrastructure as code plugin"""

    def on_group_finalisation(self):
        """Called after all group memberships have been finalised"""


def get_plugins() -> dict[str, type[CpgInfrastructurePlugin]]:
    """
    You can't just import from the submodules because it would cause a circular
    import error. So we manually load the file and then find the class that
    inherits from CpgInfrastructurePlugin.
    """

    plugins = {}

    for entry_point in importlib.metadata.entry_points().get(
        PLUGIN_ENTRYPOINT_NAME, []
    ):
        plugins[entry_point.name] = entry_point.load()

    return plugins
