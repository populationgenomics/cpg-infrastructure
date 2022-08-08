from typing import Any

import pulumi
import pulumi_gcp as gcp

from cpg_infra.abstraction.base import CloudInfraBase, UNDELETE_PERIOD_IN_DAYS, ARCHIVE_PERIOD_IN_DAYS, TMP_BUCKET_PERIOD_IN_DAYS, BucketPermission
from cpg_infra.config import CPGDatasetConfig, CPGDatasetComponents

GCP_CUSTOMER_ID = "C010ys3gt"


class GcpInfrastructure(CloudInfraBase):
    @staticmethod
    def name():
        return "gcp"

    def __init__(self, config: CPGDatasetConfig):
        super().__init__(config)

        self.region = "australia-southeast1"

        self._svc_cloudresourcemanager = gcp.projects.Service(
            "cloudresourcemanager-service",
            service="cloudresourcemanager.googleapis.com",
            disable_on_destroy=False,
        )

        self._svc_cloudidentity = gcp.projects.Service(
            "cloudidentity-service",
            service="cloudidentity.googleapis.com",
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
        )

        self._svc_serviceusage = gcp.projects.Service(
            "serviceusage-service",
            service="serviceusage.googleapis.com",
            disable_on_destroy=False,
        )

        self._svc_lifescience = gcp.projects.Service(
            "lifesciences-service",
            service="lifesciences.googleapis.com",
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_serviceusage]),
        )

        self._svc_dataproc = None
        if CPGDatasetComponents.SPARK in self.components:
            self._svc_dataproc = gcp.projects.Service(
                "dataproc-service",
                service="dataproc.googleapis.com",
                disable_on_destroy=False,
                opts=pulumi.resource.ResourceOptions(
                    depends_on=[self._svc_cloudresourcemanager]
                ),
            )

    def bucket_rule_undelete(self, days=UNDELETE_PERIOD_IN_DAYS) -> Any:
        return gcp.storage.BucketLifecycleRuleArgs(
            action=gcp.storage.BucketLifecycleRuleActionArgs(type="Delete"),
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(
                days_since_noncurrent_time=days, with_state="ARCHIVED"
            ),
        )

    def bucket_rule_archive(self, days=ARCHIVE_PERIOD_IN_DAYS) -> Any:
        return gcp.storage.BucketLifecycleRuleArgs(
                action=gcp.storage.BucketLifecycleRuleActionArgs(
                    type='SetStorageClass', storage_class='ARCHIVE'
                ),
                condition=gcp.storage.BucketLifecycleRuleConditionArgs(age=days),
            )

    def bucket_rule_temporary(self, days=TMP_BUCKET_PERIOD_IN_DAYS) -> Any:
        return gcp.storage.BucketLifecycleRuleArgs(
            action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(
                age=days
            ),
        )

    def create_bucket(
        self, name: str, lifecycle_rules: list, unique=False, requester_pays=False
    ) -> Any:
        unique_bucket_name = name
        if not unique:
            unique_bucket_name = f"cpg-{self.dataset}-{name}"
        return gcp.storage.Bucket(
            unique_bucket_name,
            name=unique_bucket_name,
            location=self.region,
            uniform_bucket_level_access=True,
            # versioning=gcp.storage.BucketVersioningArgs(enabled=enable_versioning),
            labels={"bucket": unique_bucket_name},
            lifecycle_rules=lifecycle_rules,
        )

    def get_member_key(self, member):
        if isinstance(member, gcp.serviceaccount.Account):
            return pulumi.Output.concat("serviceAccount:", member.email)

        if isinstance(member, gcp.cloudidentity.Group):
            return member.group_key

        raise NotImplementedError(f"Not valid for type {type(member)}")

    @staticmethod
    def bucket_membership_to_role(membership: BucketPermission):
        # TODO: fix organization id
        organization_id = 'TEMPORARY'
        if membership == BucketPermission.MUTATE:
            return 'roles/storage.admin'
        if membership == BucketPermission.APPEND:
            return f'{organization_id}/roles/StorageViewerAndCreator'
        if membership == BucketPermission.READ:
            return f'{organization_id}/roles/StorageObjectAndBucketViewer'
        if membership == BucketPermission.LIST:
            return f'{organization_id}/roles/StorageLister'

    def add_member_to_bucket(
        self, resource_key: str, bucket, member, membership: BucketPermission
    ) -> Any:

        gcp.storage.BucketIAMMember(
            resource_key,
            bucket=bucket.name,
            role=self.bucket_membership_to_role(membership),
            member=self.get_member_key(member),
        )

    def create_machine_account(self, name: str) -> Any:
        return gcp.serviceaccount.Account(
            f'service-account-{name}',
            account_id=name,
            display_name=name,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member
    ) -> Any:
        gcp.serviceaccount.IAMMember(
            resource_key,
            service_account_id=self.get_member_key(machine_account),
            # pulumi.Output.concat(
            #     'projects/', project_id, '/serviceAccounts/', service_account
            # ),
            role="roles/iam.serviceAccountUser",
            member=self.get_member_key(member),
        )

    def create_group(self, name: str) -> Any:
        mail = f"{name}@populationgenomics.org.au"
        return gcp.cloudidentity.Group(
            name,
            display_name=name,
            group_key=gcp.cloudidentity.GroupGroupKeyArgs(id=mail),
            labels={"cloudidentity.googleapis.com/groups.discussion_forum": ""},
            parent=f"customers/{GCP_CUSTOMER_ID}",
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def add_group_member(self, resource_key: str, group, member) -> Any:
        gcp.cloudidentity.GroupMembership(
            resource_key,
            group=group.id,
            preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
                id=member
            ),
            roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name="MEMBER")],
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def create_secret(self, name: str) -> Any:
        pass

    def add_secret_member(self, resource_key: str, secret, member, membership) -> Any:
        pass

    def add_member_to_artifact_registry(
        self, resource_key: str, artifact_registry, member
    ) -> Any:
        pass

    def add_member_to_lifescience_api(self, resource_key: str, service_account):
        gcp.projects.IAMMember(
            resource_key,
            role="roles/lifesciences.workflowsRunner",
            member=pulumi.Output.concat("serviceAccount:", service_account),
        )
