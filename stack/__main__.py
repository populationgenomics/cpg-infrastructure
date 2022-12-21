# pylint: disable=import-error
"""
This file is run by Pulumi to generate the infrastructure for a dataset
"""

import os
import pulumi
import yaml

from cpg_utils.config import get_config
from cpg_infra.config import CPGInfrastructureConfig
from cpg_infra.driver import CPGInfrastructure


def from_pulumi():
    """
    This function is called (implicitly) by Pulumi, it:
     * gets the config,
     * sets up the environment
     * builds the stack using the driver functions
    """
    config = CPGInfrastructureConfig.from_dict(get_config())

    pconfig = pulumi.Config()

    dataset_config_path = pconfig.get('dataset_config_path')
    with open(dataset_config_path, encoding='utf-8') as f:
        dataset_configs = yaml.safe_load(f)
    CPGInfrastructure(config).deploy_all_from_dataset_configs(dataset_configs)


if __name__ == '__main__':
    if os.getenv('DEBUG'):
        import debugpy

        debugpy.listen(('localhost', 5678))
        print('debugpy is listening, attach by pressing F5 or ►')

        debugpy.wait_for_client()
        print('Attached to debugpy!')

    from_pulumi()
