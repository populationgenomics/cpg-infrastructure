# pylint: disable=import-error
"""
This file is run by Pulumi to generate the infrastructure for a dataset
"""

import os
import pulumi

from cpg_utils.config import get_config
from cpg_infra.config import CPGInfrastructureConfig, CPGDatasetConfig
from cpg_infra.driver import CpgDatasetInfrastructure


def from_pulumi():
    """
    This function is called (implicitly) by Pulumi, it:
     * gets the config,
     * sets up the environment
     * builds the stack using the driver functions
    """
    config = CPGInfrastructureConfig.from_dict(get_config())
    pconfig = pulumi.Config()
    dataset_config = CPGDatasetConfig.from_pulumi(pconfig, dataset=pulumi.get_stack())
    CpgDatasetInfrastructure.deploy_all_from_config(config, dataset_config)


if __name__ == '__main__':
    if os.getenv('DEBUG'):
        import debugpy

        debugpy.listen(('localhost', 5678))
        print('debugpy is listening, attach by pressing F5 or ►')

        debugpy.wait_for_client()
        print('Attached to debugpy!')

    from_pulumi()
