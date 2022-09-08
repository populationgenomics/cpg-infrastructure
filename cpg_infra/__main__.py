import pulumi
from cpg_utils.config import get_config

from cpg_infra.driver import (
    CpgDatasetInfrastructure,
    CPGInfrastructureConfig,
    CPGDatasetConfig,
)

# NOTE: Uncomment the below code when launching pulumi locally
# First, run the Remote Attach debugger then
# after running `pulumi up` or an equivalent command, hit F5 to connect the
# vscode debugger. Helpful for finding hidden pulumi errors

def from_pulumi():
    pconfig = pulumi.Config()
    config = CPGInfrastructureConfig.from_dict(get_config())
    dataset_config = CPGDatasetConfig.from_pulumi(pconfig, dataset=pulumi.get_stack())
    CpgDatasetInfrastructure.deploy_all_from_config(config, dataset_config)


if __name__ == '__main__':
    import debugpy

    debugpy.listen(("localhost", 5678))
    print("debugpy is listening, attach by pressing F5 or ►")

    debugpy.wait_for_client()
    print("Attached to debugpy!")

    from_pulumi()
