import pulumi
from cpg_infra.driver import CPGInfrastructure, CPGDatasetConfig

# NOTE: Uncomment the below code when launching pulumi locally
# after running `pulumi up` or an equivalent command, then hit F5 to connect the
# vscode debugger. Helpful for finding hidden pulumi errors

# import debugpy
#
# debugpy.listen(("0.0.0.0", 5678))
# print("debugpy is listening, attach by pressing F5 or ►")
#
# debugpy.wait_for_client()
# print("Attached to debugpy!")

def from_pulumi():
    pconfig = pulumi.Config()
    config = CPGDatasetConfig.from_pulumi(pconfig, dataset=pulumi.get_stack())
    CPGInfrastructure.deploy_all_from_config(config)


if __name__ == '__main__':
    from_pulumi()