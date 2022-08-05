import pulumi

from cpg_infra.abstraction.azure import AzureInfra
from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.abstraction.base import CloudInfraBase, DevInfra

def main(dataset='test'):
    config = {'dataset': dataset}
    infras: list[CloudInfraBase] = [i(config) for i in (DevInfra, GcpInfrastructure, AzureInfra)]

    for base in infras:

        main_upload_account = base.create_machine_account('main-upload')
        main_upload_buckets = {
            'main-upload': base.create_bucket('main-upload', lifecycle_rules=[])
        }

        test_upload_bucket = base.create_bucket('test-upload', lifecycle_rules=[])
        for bname, main_upload_bucket in main_upload_buckets.items():
            base.add_member_to_bucket(
                f'main-upload-service-account-{bname}-bucket-creator',
                bucket=main_upload_bucket,
                member=main_upload_account
            )

if __name__ == '__main__':
    class MyMocks(pulumi.runtime.Mocks):
        def new_resource(self, args: pulumi.runtime.MockResourceArgs):
            return [args.name + '_id', args.inputs]

        def call(self, args: pulumi.runtime.MockCallArgs):
            return {}

    pulumi.runtime.set_mocks(
        MyMocks(),
        preview=False,  # Sets the flag `dry_run`, which is true at runtime during a preview.
    )

    main()