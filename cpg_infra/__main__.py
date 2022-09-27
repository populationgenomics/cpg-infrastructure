# pylint: disable=import-error
"""
This file is run by Pulumi to generate the infrastructure for a dataset
"""

import time
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
    """
    This function is called (implicitly) by Pulumi, it:
     * gets the config,
     * sets up the environment
     * builds the stack using the driver functions
    """
    pconfig = pulumi.Config()
    config = CPGInfrastructureConfig.from_dict(get_config())
    dataset_config = CPGDatasetConfig.from_pulumi(pconfig, dataset=pulumi.get_stack())
    CpgDatasetInfrastructure.deploy_all_from_config(config, dataset_config)


def wait(seconds):
    while seconds >= 0:
        if debugpy.is_client_connected():
            break
        time.sleep(1)
        seconds -= 1

if __name__ == '__main__':
    import debugpy

    # debugpy.listen(("localhost", 5678))
    # print("debugpy is listening, attach by pressing F5 or ►")

    # wait(10)
    # debugpy.wait_for_client()
    # print("Attached to debugpy!")

    from_pulumi()
