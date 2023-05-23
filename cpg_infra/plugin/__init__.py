from cpg_infra.config import CPGInfrastructureConfig

# from inspect import isclass, getmro
from pathlib import Path
# import importlib.util

cpg_infra_path = str(Path(__file__).parent.parent.parent.absolute())
PLUGIN_ENTRYPOINT_NAME = 'cpginfra.plugins'

class CpgInfrastructurePlugin:
    """Billing aggregator Infrastructure (as code) for Pulumi"""

    def __init__(self, config: CPGInfrastructureConfig):
        self.config = config

    def main(self):
        pass


def get_plugins() -> dict:
    """
    You can't just import from the submodules because it would cause a circular
    import error. So we manually load the file and then find the class that
    inherits from CpgInfrastructurePlugin.
    """
    import pkg_resources

    plugins = {}

    for entry_point in pkg_resources.iter_entry_points(PLUGIN_ENTRYPOINT_NAME):
        plugins[entry_point.name] = entry_point.load()

    return plugins


    # expand path to include root of cpg-infrastructure directory so we can
    sys.path.append(cpg_infra_path)

    from metamist.infrastructure.driver import MetamistInfrastructure
    from cpg_infra.billing_aggregator.driver import BillingAggregator

    modules = [BillingAggregator, MetamistInfrastructure]

    return modules

    # root = Path(__file__).parent.parent
    # known_paths = {
    #     'metamist_infrastructure': str(root / 'metamist/infrastructure/driver.py'),
    # }
    # for key, module_path in known_paths.items():
    #     spec = importlib.util.spec_from_file_location(key, module_path)
    #     foo = importlib.util.module_from_spec(spec)
    #
    #     spec.loader.exec_module(foo)
    #
    #     for item in foo.__dict__.values():
    #         if isclass(item) and item != CpgInfrastructurePlugin:
    #             mro = getmro(item)
    #             if 'CpgInfrastructurePlugin' in [cls.__name__ for cls in mro[1:]]:
    #                 modules.append(item)
    #
    # return modules
