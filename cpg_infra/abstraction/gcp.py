from functools import lru_cache
from typing import Any

import pulumi
import pulumi_gcp as gcp

from cpg_infra.abstraction.base import (
    CloudInfraBase,
    UNDELETE_PERIOD_IN_DAYS,
    ARCHIVE_PERIOD_IN_DAYS,
    TMP_BUCKET_PERIOD_IN_DAYS,
    BucketPermission,
)
from cpg_infra.config import CPGDatasetConfig, CPGDatasetComponents, DOMAIN

GCP_CUSTOMER_ID = "C010ys3gt"


class GcpInfrastructure(CloudInfraBase):
    @staticmethod
    def name():
        return "gcp"

    def __init__(self, config: CPGDatasetConfig):
        super().__init__(config)

        self.region = "australia-southeast1"
        self.project = gcp.organizations.get_project().project_id
        self.organization = gcp.organizations.get_organization(domain=DOMAIN)

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

        self._svc_secretmanager = gcp.projects.Service(
            'secretmanager-service',
            service='secretmanager.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
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

    @property
    @lru_cache
    def _svc_lifescienceapi(self):
        return gcp.projects.Service(
            'lifesciences-service',
            service='lifesciences.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_serviceusage]),
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
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(age=days),
        )

    def create_bucket(
        self,
        name: str,
        lifecycle_rules: list,
        unique=False,
        requester_pays=False,
        versioning: bool = True,
    ) -> Any:
        unique_bucket_name = name
        if not unique:
            unique_bucket_name = f"cpg-{self.dataset}-{name}"
        return gcp.storage.Bucket(
            unique_bucket_name,
            name=unique_bucket_name,
            location=self.region,
            uniform_bucket_level_access=True,
            versioning=gcp.storage.BucketVersioningArgs(enabled=versioning),
            labels={"bucket": unique_bucket_name},
            lifecycle_rules=lifecycle_rules,
            requester_pays=requester_pays,
        )

    def get_member_key(self, member):
        if isinstance(member, gcp.serviceaccount.Account):
            return pulumi.Output.concat("serviceAccount:", member.email)

        if isinstance(member, gcp.cloudidentity.Group):
            return pulumi.Output.concat('group:', member.group_key.id)

        if isinstance(member, str):
            return member

        if isinstance(member, pulumi.Output):
            return member

        raise NotImplementedError(f"Not valid for type {type(member)}")

    def bucket_membership_to_role(self, membership: BucketPermission):
        # TODO: fix organization id
        if membership == BucketPermission.MUTATE:
            return 'roles/storage.admin'
        if membership == BucketPermission.APPEND:
            return f'{self.organization.id}/roles/StorageViewerAndCreator'
        if membership == BucketPermission.READ:
            return f'{self.organization.id}/roles/StorageObjectAndBucketViewer'
        if membership == BucketPermission.LIST:
            return f'{self.organization.id}/roles/StorageLister'

    def add_member_to_bucket(
        self, resource_key: str, bucket, member, membership: BucketPermission
    ) -> Any:
        kwargs = dict(
            bucket=bucket,
            member=member,
            role=membership,
        )
        # print(f'Creating BucketIAMMember ({resource_key}): {kwargs}', flush=True)
        gcp.storage.BucketIAMMember(
            resource_key,
            bucket=bucket.name,
            member=self.get_member_key(member),
            role=self.bucket_membership_to_role(membership),
        )

    def create_machine_account(self, name: str, project=None) -> Any:
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
        return gcp.secretmanager.Secret(
            name,
            secret_id=name,
            replication=gcp.secretmanager.SecretReplicationArgs(
                user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
                    replicas=[
                        gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                            location=self.region,
                        ),
                    ],
                ),
            ),
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_secretmanager]),
        )

    def add_secret_member(self, resource_key: str, secret, member, membership) -> Any:
        pass

    def add_member_to_artifact_registry(
        self, resource_key: str, artifact_registry, member
    ) -> Any:
        pass

    # region GCP SPECIFIC

    def add_member_to_lifescience_api(self, resource_key: str, account):
        gcp.projects.IAMMember(
            resource_key,
            role="roles/lifesciences.workflowsRunner",
            member=self.get_member_key(account),
            project=self.project,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_lifescienceapi]),
        )

    #
    def add_member_to_dataproc_api(self, resource_key: str, account):
        gcp.projects.IAMMember(
            resource_key,
            role='roles/dataproc.worker',
            member=self.get_member_key(account),
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_lifescienceapi]),
        )

    def add_cloudrun_invoker(
        self, resource_key: str, *, service: str, project: str, member
    ):
        gcp.cloudrun.IamMember(
            resource_key,
            location=self.region,
            project=project,
            service=service,
            role='roles/run.invoker',
            member=self.get_member_key(member),
        )

    def add_project_role(
        self, resource_key: str, *, project: str, member: any, role: str
    ):
        gcp.projects.IAMMember(
            resource_key,
            project=project,
            role=role,
            member=self.get_member_key(member),
        )

    # endregion GCP SPECIFIC
