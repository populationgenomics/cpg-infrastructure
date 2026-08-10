# flake8: noqa: F401
"""
CPG Dataset infrastructure - driver package.

Re-exports the classes, constants, and helpers that previously lived in
``cpg_infra/driver.py`` so that ``from cpg_infra.driver import ...``
continues to work.
"""

import cpg_utils.config

from cpg_infra.config import (
    CloudName,
    CPGDatasetComponents,
    CPGDatasetConfig,
    CPGInfrastructureConfig,
    CPGInfrastructureUser,
    HailAccount,
)
from cpg_infra.driver.constants import (
    METAMIST_PERMISSIONS,
    NAME_TO_INFRA_CLASS,
    NON_NAME_REGEX,
    SM_MAIN_CONTRIBUTE,
    SM_MAIN_READ,
    SM_MAIN_WRITE,
    SM_TEST_CONTRIBUTE,
    SM_TEST_READ,
    SM_TEST_WRITE,
    TOML_CONFIG_JOINER,
    AccessLevel,
    access_levels,
    compute_hash,
    dict_to_toml,
)
from cpg_infra.driver.cpg_dataset_cloud_infrastructure import (
    CPGDatasetCloudInfrastructure,
)
from cpg_infra.driver.cpg_dataset_infrastructure import CPGDatasetInfrastructure
from cpg_infra.driver.cpg_infrastructure import CPGInfrastructure
from cpg_infra.driver.group import Group
from cpg_infra.driver.group_member import GroupMember
from cpg_infra.driver.group_provider import GroupProvider
from cpg_infra.driver.main_upload_bucket import MainUploadBucket
from cpg_infra.driver.sample_metadata_accessor_membership import (
    SampleMetadataAccessorMembership,
)


def test():
    infra_config_dict = dict(cpg_utils.config.get_config(print_config=False))
    infra_config_dict['infrastructure']['reference_dataset'] = 'fewgenomes'
    infra_config = CPGInfrastructureConfig.model_validate(
        infra_config_dict.get('infrastructure', infra_config_dict),
    )

    configs = [
        CPGDatasetConfig(
            dataset='fewgenomes',
            deploy_locations=['dry-run'],
            gcp=CPGDatasetConfig.Gcp(
                project='test-project',
            ),
            budgets={'dry-run': CPGDatasetConfig.Budget(monthly_budget=100)},
        ),
    ]
    infra = CPGInfrastructure(infra_config, configs)
    infra.main()


if __name__ == '__main__':
    test()
