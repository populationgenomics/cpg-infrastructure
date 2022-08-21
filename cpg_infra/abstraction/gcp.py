import base64
from functools import lru_cache
from typing import Any, Callable

import pulumi
import pulumi_gcp as gcp

from cpg_infra.abstraction.base import (
    CloudInfraBase,
    UNDELETE_PERIOD_IN_DAYS,
    ARCHIVE_PERIOD_IN_DAYS,
    TMP_BUCKET_PERIOD_IN_DAYS,
    BucketPermission,
    SecretMembership,
    ContainerRegistryMembership,
)
from cpg_infra.config import CPGDatasetConfig, DOMAIN, CPGInfrastructureConfig

GCP_CUSTOMER_ID = 'C010ys3gt'


class GcpInfrastructure(CloudInfraBase):
    @staticmethod
    def name():
        return 'gcp'

    def __init__(
        self, config: CPGInfrastructureConfig, dataset_config: CPGDatasetConfig
    ):
        super().__init__(config, dataset_config)

        self.region = 'australia-southeast1'
        self.organization = gcp.organizations.get_organization(domain=DOMAIN)
        self.project = gcp.organizations.get_project().project_id

        self._svc_cloudresourcemanager = gcp.projects.Service(
            'cloudresourcemanager-service',
            service='cloudresourcemanager.googleapis.com',
            disable_on_destroy=False,
        )

        self._svc_cloudidentity = gcp.projects.Service(
            'cloudidentity-service',
            service='cloudidentity.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
        )

        self._svc_iam = gcp.projects.Service(
            'iam-service',
            service='iam.googleapis.com',
            disable_on_destroy=False,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
        )

        self._svc_serviceusage = gcp.projects.Service(
            'serviceusage-service',
            service='serviceusage.googleapis.com',
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

    @property
    @lru_cache()
    def _svc_dataproc(self):
        return gcp.projects.Service(
            'dataproc-service',
            service='dataproc.googleapis.com',
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
            action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
            condition=gcp.storage.BucketLifecycleRuleConditionArgs(
                days_since_noncurrent_time=days, with_state='ARCHIVED'
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
        project: str = None,
    ) -> Any:
        unique_bucket_name = name
        if not unique:
            unique_bucket_name = (
                f'{self.config.dataset_storage_prefix}{self.dataset}-{name}'
            )
        return gcp.storage.Bucket(
            unique_bucket_name,
            name=unique_bucket_name,
            location=self.region,
            uniform_bucket_level_access=True,
            versioning=gcp.storage.BucketVersioningArgs(enabled=versioning),
            labels={'bucket': unique_bucket_name},
            lifecycle_rules=lifecycle_rules,
            requester_pays=requester_pays,
            project=project or self.project,
        )

    def get_member_key(self, member):
        if isinstance(member, gcp.serviceaccount.Account):
            return pulumi.Output.concat('serviceAccount:', member.email)

        if isinstance(member, gcp.cloudidentity.Group):
            return pulumi.Output.concat('group:', member.group_key.id)

        if isinstance(member, gcp.storage.Bucket):
            return member.name

        if isinstance(member, str):
            if member.endswith('.iam.gserviceaccount.com') and not member.startswith(
                'serviceAccount:'
            ):
                return f'serviceAccount:{member}'

            return member

        if isinstance(member, pulumi.Output):
            return member

        raise NotImplementedError(f'Invalid member type {type(member)}')

    def get_preferred_group_membership_key(self, member):
        if isinstance(member, gcp.cloudidentity.Group):
            return member.group_key.id
        if isinstance(member, gcp.serviceaccount.Account):
            return member.email
        if isinstance(member, str):
            return member

        raise NotImplementedError(
            f'Invalid preferred GroupMembership type {type(member)}'
        )

    def get_group_key(self, group):
        if isinstance(group, (gcp.serviceaccount.Account, gcp.storage.Bucket)):
            raise ValueError(f'Incorrect type for group key: {type(group)}')

        if isinstance(group, gcp.cloudidentity.Group):
            return group.id
            # return pulumi.Output.concat('group:', group.group_key.id)

        if isinstance(group, str):
            if group.endswith('@populationgenomics.org.au') and not group.startswith(
                'group:'
            ):
                return f'group:{group}'

            return group

        if isinstance(group, pulumi.Output):
            return group

        raise NotImplementedError(f'Not valid for type {type(group)}')

    def get_secret_key(self, secret):
        if isinstance(secret, gcp.secretmanager.Secret):
            return secret.id

        if isinstance(secret, str):
            return secret

        raise NotImplementedError(f'Not valid for type {type(secret)}')

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
        gcp.storage.BucketIAMMember(
            resource_key,
            bucket=self.get_member_key(bucket),
            member=self.get_member_key(member),
            role=self.bucket_membership_to_role(membership),
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def give_member_ability_to_list_buckets(
        self, resource_key: str, member, project: str = None
    ):
        gcp.projects.IAMMember(
            resource_key,
            role=self.bucket_membership_to_role(BucketPermission.LIST),
            member=self.get_member_key(member),
            project=project or self.project,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def create_machine_account(
        self, name: str, project: str = None, *, resource_key: str = None
    ) -> Any:
        return gcp.serviceaccount.Account(
            resource_key or f'service-account-{name}',
            account_id=name,
            # display_name=name,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_iam]),
            project=project,
        )

    def add_member_to_machine_account_access(
        self, resource_key: str, machine_account, member, project: str = None
    ) -> Any:
        gcp.serviceaccount.IAMMember(
            resource_key,
            service_account_id=machine_account.name,
            role='roles/iam.serviceAccountUser',
            member=self.get_member_key(member),
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudidentity, self._svc_iam]
            ),
        )

    def get_credentials_for_machine_account(self, resource_key, account):
        return gcp.serviceaccount.Key(
            resource_key,
            service_account_id=account.email,
        ).private_key.apply(lambda s: base64.b64decode(s).decode('utf-8'))

    def create_group(self, name: str) -> Any:
        mail = f'{name}@populationgenomics.org.au'
        return gcp.cloudidentity.Group(
            name,
            display_name=name,
            group_key=gcp.cloudidentity.GroupGroupKeyArgs(id=mail),
            labels={'cloudidentity.googleapis.com/groups.discussion_forum': ''},
            parent=f'customers/{GCP_CUSTOMER_ID}',
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def add_group_member(self, resource_key: str, group, member) -> Any:
        gcp.cloudidentity.GroupMembership(
            resource_key,
            group=self.get_group_key(group),
            preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
                id=self.get_preferred_group_membership_key(member)
            ),
            roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_cloudidentity]),
        )

    def create_secret(self, name: str, project: str = None) -> Any:
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
            project=project or self.project,
        )

    def add_secret_member(
        self,
        resource_key: str,
        secret,
        member,
        membership: SecretMembership,
        project: str = None,
    ) -> Any:

        if membership == SecretMembership.ADMIN:
            role = 'roles/secretmanager.secretVersionManager'
        elif membership == SecretMembership.ACCESSOR:
            role = 'roles/secretmanager.secretAccessor'
        else:
            raise ValueError(f'Unrecognised secret membership type: {membership}')

        gcp.secretmanager.SecretIamMember(
            resource_key,
            project=project or self.project,
            secret_id=secret.id,
            role=role,
            member=self.get_member_key(member),
        )

    def add_secret_version(
        self,
        resource_key: str,
        secret: Any,
        contents: Any,
        processor: Callable[[Any], Any] = None,
    ):
        return gcp.secretmanager.SecretVersion(
            resource_key, secret=secret.id, secret_data=contents
        )

    def add_member_to_container_registry(
        self, resource_key: str, registry, member, membership, project=None
    ) -> Any:

        if membership == ContainerRegistryMembership.READER:
            role = 'roles/artifactregistry.reader'
        elif membership == ContainerRegistryMembership.APPEND:
            role = 'roles/artifactregistry.writer'
        else:
            raise ValueError(f'Unrecognised group membership type: {membership}')

        gcp.artifactregistry.RepositoryIamMember(
            resource_key,
            project=project or self.project,
            location=self.region,
            repository=registry,
            role=role,
            member=self.get_member_key(member),
        )

    # region GCP SPECIFIC

    def add_member_to_lifescience_api(self, resource_key: str, account):
        gcp.projects.IAMMember(
            resource_key,
            role='roles/lifesciences.workflowsRunner',
            member=self.get_member_key(account),
            project=self.project,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_lifescienceapi]),
        )

    #
    def add_member_to_dataproc_api(self, resource_key: str, account, role: str):
        assert role in ('worker', 'admin')

        gcp.projects.IAMMember(
            resource_key,
            role=f'roles/dataproc.{role}',
            member=self.get_member_key(account),
            project=self.project,
            opts=pulumi.resource.ResourceOptions(depends_on=[self._svc_dataproc]),
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
        self, resource_key: str, *, member: Any, role: str, project: str = None
    ):
        gcp.projects.IAMMember(
            resource_key,
            project=project or self.project,
            role=role,
            member=self.get_member_key(member),
        )

    # endregion GCP SPECIFIC
