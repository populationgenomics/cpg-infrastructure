"""Pulumi stack to set up buckets and permission groups."""

import base64
import pulumi
import pulumi_gcp as gcp

DOMAIN = 'populationgenomics.org.au'
CUSTOMER_ID = 'C010ys3gt'
REGION = 'australia-southeast1'
ANALYSIS_RUNNER_PROJECT = 'analysis-runner'
CPG_COMMON_PROJECT = 'cpg-common'
HAIL_PROJECT = 'hail-295901'
ANALYSIS_RUNNER_SERVICE_ACCOUNT = (
    'analysis-runner-server@analysis-runner.iam.gserviceaccount.com'
)
WEB_SERVER_SERVICE_ACCOUNT = 'web-server@analysis-runner.iam.gserviceaccount.com'
ACCESS_GROUP_CACHE_SERVICE_ACCOUNT = (
    'access-group-cache@analysis-runner.iam.gserviceaccount.com'
)
REFERENCE_BUCKET_NAME = 'cpg-reference'
NOTEBOOKS_PROJECT = 'notebooks-314505'
# cromwell-submission-access@populationgenomics.org.au
CROMWELL_ACCESS_GROUP_ID = 'groups/03cqmetx2922fyu'
CROMWELL_RUNNER_ACCOUNT = 'cromwell-runner@cromwell-305305.iam.gserviceaccount.com'


def main():  # pylint: disable=too-many-locals
    """Main entry point."""

    # Fetch configuration.
    config = pulumi.Config()
    enable_release = config.get_bool('enable_release')
    archive_age = config.get_int('archive_age') or 30

    dataset = pulumi.get_stack()

    project_id = gcp.organizations.get_project().project_id

    # The Cloud Resource Manager API is required for the Cloud Identity API.
    cloudresourcemanager = gcp.projects.Service(
        'cloudresourcemanager-service',
        service='cloudresourcemanager.googleapis.com',
        disable_on_destroy=False,
    )

    # The Cloud Identity API is required for creating access groups and service accounts.
    cloudidentity = gcp.projects.Service(
        'cloudidentity-service',
        service='cloudidentity.googleapis.com',
        disable_on_destroy=False,
        opts=pulumi.resource.ResourceOptions(depends_on=[cloudresourcemanager]),
    )

    # The Hail service account email addresses associated with the three access levels.
    hail_service_account_test = config.require('hail_service_account_test')
    hail_service_account_standard = config.require('hail_service_account_standard')
    hail_service_account_full = config.require('hail_service_account_full')

    hail_service_accounts = [
        ('test', hail_service_account_test),
        ('standard', hail_service_account_standard),
        ('full', hail_service_account_full),
    ]

    # Create Cromwell service accounts.
    cromwell_service_accounts = []
    for access_level in 'test', 'standard', 'full':
        account = gcp.serviceaccount.Account(
            f'cromwell-service-account-{access_level}',
            account_id=f'cromwell-{access_level}',
            opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
        )

        cromwell_service_accounts.append((access_level, account.email))

    service_accounts = [
        ('hail', access_level, service_account)
        for access_level, service_account in hail_service_accounts
    ] + [
        ('cromwell', access_level, service_account)
        for access_level, service_account in cromwell_service_accounts
    ]

    def bucket_name(kind: str) -> str:
        """Returns the bucket name for the given dataset."""
        return f'cpg-{dataset}-{kind}'

    def create_bucket(name: str, **kwargs) -> gcp.storage.Bucket:
        """Returns a new GCS bucket."""
        return gcp.storage.Bucket(
            name,
            name=name,
            location=REGION,
            uniform_bucket_level_access=True,
            versioning=gcp.storage.BucketVersioningArgs(enabled=True),
            labels={'bucket': name},
            **kwargs,
        )

    def bucket_member(*args, **kwargs):
        """Wraps gcp.storage.BucketIAMMember.

        When resources are renamed, it can be useful to explicitly apply changes in two
        phases: delete followed by create; that's opposite of the default create followed by
        delete, which can end up with missing permissions. To implement the first phase
        (delete), simply change this implementation to a no-op temporarily.
        """
        gcp.storage.BucketIAMMember(*args, **kwargs)

    undelete_rule = gcp.storage.BucketLifecycleRuleArgs(
        action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
        condition=gcp.storage.BucketLifecycleRuleConditionArgs(
            age=30, with_state='ARCHIVED'
        ),
    )

    upload_account = gcp.serviceaccount.Account(
        'upload-service-account',
        account_id='upload',
        display_name='upload',
        opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    )

    upload_bucket = create_bucket(
        bucket_name('upload'), lifecycle_rules=[undelete_rule]
    )

    bucket_member(
        'upload-service-account-upload-bucket-creator',
        bucket=upload_bucket.name,
        role='roles/storage.objectCreator',
        member=pulumi.Output.concat('serviceAccount:', upload_account.email),
    )

    archive_bucket = create_bucket(
        bucket_name('archive'),
        lifecycle_rules=[
            gcp.storage.BucketLifecycleRuleArgs(
                action=gcp.storage.BucketLifecycleRuleActionArgs(
                    type='SetStorageClass', storage_class='ARCHIVE'
                ),
                condition=gcp.storage.BucketLifecycleRuleConditionArgs(age=archive_age),
            ),
            undelete_rule,
        ],
    )

    test_bucket = create_bucket(bucket_name('test'), lifecycle_rules=[undelete_rule])

    test_tmp_bucket = create_bucket(
        bucket_name('test-tmp'),
        lifecycle_rules=[
            gcp.storage.BucketLifecycleRuleArgs(
                action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
                condition=gcp.storage.BucketLifecycleRuleConditionArgs(
                    age=30, with_state='LIVE'
                ),
            ),
            undelete_rule,
        ],
    )

    test_metadata_bucket = create_bucket(
        bucket_name('test-metadata'), lifecycle_rules=[undelete_rule]
    )

    test_web_bucket = create_bucket(
        bucket_name('test-web'), lifecycle_rules=[undelete_rule]
    )

    main_bucket = create_bucket(bucket_name('main'), lifecycle_rules=[undelete_rule])

    main_tmp_bucket = create_bucket(
        bucket_name('main-tmp'),
        lifecycle_rules=[
            gcp.storage.BucketLifecycleRuleArgs(
                action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
                condition=gcp.storage.BucketLifecycleRuleConditionArgs(
                    age=30, with_state='LIVE'
                ),
            ),
            undelete_rule,
        ],
    )

    main_metadata_bucket = create_bucket(
        bucket_name('main-metadata'), lifecycle_rules=[undelete_rule]
    )

    main_web_bucket = create_bucket(
        bucket_name('main-web'), lifecycle_rules=[undelete_rule]
    )

    def group_mail(kind: str) -> str:
        """Returns the email address of a permissions group."""
        return f'{dataset}-{kind}@{DOMAIN}'

    def create_group(mail: str) -> gcp.cloudidentity.Group:
        """Returns a new Cloud Identity group for the given email address."""
        name = mail.split('@')[0]
        return gcp.cloudidentity.Group(
            name,
            display_name=name,
            group_key=gcp.cloudidentity.GroupGroupKeyArgs(id=mail),
            labels={'cloudidentity.googleapis.com/groups.discussion_forum': ''},
            parent=f'customers/{CUSTOMER_ID}',
            opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
        )

    def add_bucket_permissions(
        name: str, group: gcp.cloudidentity.Group, bucket: gcp.storage.Bucket, role: str
    ) -> gcp.storage.BucketIAMMember:
        """Returns GCS bucket permissions for the given group."""
        return bucket_member(
            name,
            bucket=bucket.name,
            role=role,
            member=pulumi.Output.concat('group:', group.group_key.id),
        )

    access_group = create_group(group_mail('access'))

    # This secret is used as a fast cache for checking memberships in the above group.
    access_group_cache_secret = gcp.secretmanager.Secret(
        f'access-group-cache-secret',
        secret_id=f'{dataset}-access-members-cache',
        project=ANALYSIS_RUNNER_PROJECT,
        replication=gcp.secretmanager.SecretReplicationArgs(
            user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
                replicas=[
                    gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                        location='australia-southeast1',
                    ),
                ],
            ),
        ),
    )

    gcp.secretmanager.SecretIamMember(
        f'access-group-cache-secret-accessor',
        project=ANALYSIS_RUNNER_PROJECT,
        secret_id=access_group_cache_secret.id,
        role='roles/secretmanager.secretAccessor',
        member=f'serviceAccount:{ACCESS_GROUP_CACHE_SERVICE_ACCOUNT}',
    )

    gcp.secretmanager.SecretIamMember(
        f'access-group-cache-secret-version-adder',
        project=ANALYSIS_RUNNER_PROJECT,
        secret_id=access_group_cache_secret.id,
        role='roles/secretmanager.secretVersionAdder',
        member=f'serviceAccount:{ACCESS_GROUP_CACHE_SERVICE_ACCOUNT}',
    )

    gcp.secretmanager.SecretIamMember(
        f'analyis-runner-access-group-cache-secret-accessor',
        project=ANALYSIS_RUNNER_PROJECT,
        secret_id=access_group_cache_secret.id,
        role='roles/secretmanager.secretAccessor',
        member=f'serviceAccount:{ANALYSIS_RUNNER_SERVICE_ACCOUNT}',
    )

    listing_role = gcp.projects.IAMCustomRole(
        'storage-listing-role',
        description='Allows listing of storage objects',
        permissions=[
            'storage.objects.list',
            'storage.buckets.list',
            'storage.buckets.get',
        ],
        role_id='storageObjectLister',
        title='Storage Object Lister',
        opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    )

    gcp.projects.IAMMember(
        'project-buckets-lister',
        role=listing_role,
        member=pulumi.Output.concat('group:', access_group.group_key.id),
    )

    add_bucket_permissions(
        'access-group-test-bucket-admin',
        access_group,
        test_bucket,
        'roles/storage.admin',
    )

    add_bucket_permissions(
        'access-group-test-tmp-bucket-admin',
        access_group,
        test_tmp_bucket,
        'roles/storage.admin',
    )

    add_bucket_permissions(
        'access-group-test-metadata-bucket-admin',
        access_group,
        test_metadata_bucket,
        'roles/storage.admin',
    )

    add_bucket_permissions(
        'access-group-test-web-bucket-admin',
        access_group,
        test_web_bucket,
        'roles/storage.admin',
    )

    add_bucket_permissions(
        'access-group-main-metadata-bucket-viewer',
        access_group,
        main_metadata_bucket,
        'roles/storage.objectViewer',
    )

    add_bucket_permissions(
        'access-group-main-web-bucket-viewer',
        access_group,
        main_web_bucket,
        'roles/storage.objectViewer',
    )

    if enable_release:
        release_bucket = create_bucket(
            bucket_name('release-requester-pays'),
            lifecycle_rules=[undelete_rule],
            requester_pays=True,
        )

        add_bucket_permissions(
            'access-group-release-bucket-viewer',
            access_group,
            release_bucket,
            'roles/storage.objectViewer',
        )

        release_access_group = create_group(group_mail('release-access'))

        add_bucket_permissions(
            'release-access-group-release-bucket-viewer',
            release_access_group,
            release_bucket,
            'roles/storage.objectViewer',
        )

    bucket_member(
        'web-server-test-web-bucket-viewer',
        bucket=test_web_bucket.name,
        role='roles/storage.objectViewer',
        member=pulumi.Output.concat('serviceAccount:', WEB_SERVER_SERVICE_ACCOUNT),
    )

    bucket_member(
        'web-server-main-web-bucket-viewer',
        bucket=main_web_bucket.name,
        role='roles/storage.objectViewer',
        member=pulumi.Output.concat('serviceAccount:', WEB_SERVER_SERVICE_ACCOUNT),
    )

    # TODO(@lgruen): remove this once secrets are used for checking memberships.
    # Allow the analysis-runner to check memberships.
    gcp.cloudidentity.GroupMembership(
        'analysis-runner-restricted-member',
        group=access_group.id,
        preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
            id=ANALYSIS_RUNNER_SERVICE_ACCOUNT
        ),
        roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
    )

    # TODO(@lgruen): remove this once secrets are used for checking memberships.
    # Allow the web server to check memberships.
    gcp.cloudidentity.GroupMembership(
        'web-server-restricted-member',
        group=access_group.id,
        preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
            id=WEB_SERVER_SERVICE_ACCOUNT
        ),
        roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
    )

    # Allow the access group cache to list memberships.
    gcp.cloudidentity.GroupMembership(
        'access-group-cache-membership',
        group=access_group.id,
        preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
            id=ACCESS_GROUP_CACHE_SERVICE_ACCOUNT
        ),
        roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
    )

    for kind, access_level, service_account in service_accounts:
        # Allow the service accounts to pull images. Note that the global project will
        # refer to the dataset, but the Docker images are stored in the "analysis-runner"
        # and "cpg-common" projects' Artifact Registry repositories.
        for project in [ANALYSIS_RUNNER_PROJECT, CPG_COMMON_PROJECT]:
            gcp.artifactregistry.RepositoryIamMember(
                f'{kind}-service-account-{access_level}-images-reader-in-{project}',
                project=project,
                location=REGION,
                repository='images',
                role='roles/artifactregistry.reader',
                member=pulumi.Output.concat('serviceAccount:', service_account),
            )

        # Read access to reference data.
        bucket_member(
            f'{kind}-service-account-{access_level}-reference-bucket-viewer',
            bucket=REFERENCE_BUCKET_NAME,
            role='roles/storage.objectViewer',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

    # The bucket used for Hail Batch pipelines.
    hail_bucket = create_bucket(bucket_name('hail'), lifecycle_rules=[undelete_rule])

    for access_level, service_account in hail_service_accounts:
        # Full access to the Hail Batch bucket.
        bucket_member(
            f'hail-service-account-{access_level}-hail-bucket-admin',
            bucket=hail_bucket.name,
            role='roles/storage.admin',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

    # Permissions increase by access level:
    # - test: view / create on any "test" bucket
    # - standard: view / create on any "test" or "main" bucket
    # - full: view / create / delete anywhere

    for kind, access_level, service_account in service_accounts:
        # test bucket
        bucket_member(
            f'{kind}-service-account-{access_level}-test-bucket-admin',
            bucket=test_bucket.name,
            role='roles/storage.admin',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

        # test-tmp bucket
        bucket_member(
            f'{kind}-service-account-{access_level}-test-tmp-bucket-admin',
            bucket=test_tmp_bucket.name,
            role='roles/storage.admin',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

        # test-metadata bucket
        bucket_member(
            f'{kind}-service-account-{access_level}-test-metadata-bucket-admin',
            bucket=test_metadata_bucket.name,
            role='roles/storage.admin',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

        # test-web bucket
        bucket_member(
            f'{kind}-service-account-{access_level}-test-web-bucket-admin',
            bucket=test_web_bucket.name,
            role='roles/storage.admin',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

    # For view + create permissions, we conceptually should only have to grant the
    # roles/storage.objectViewer and roles/storage.objectCreator roles. However, Hail /
    # Spark access GCS buckets in a way that also requires storage.buckets.get permissions,
    # which is typically only included in the legacy roles. We therefore create a custom
    # role here.
    view_create_role = gcp.projects.IAMCustomRole(
        'storage-view-create-role',
        description='Allows viewing and creation of storage objects',
        permissions=[
            'storage.objects.list',
            'storage.objects.get',
            'storage.objects.create',
            'storage.buckets.get',
        ],
        role_id='storageObjectViewCreate',
        title='Storage Object Viewer + Creator',
        opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    )

    for kind, access_level, service_account in service_accounts:
        if access_level == 'standard':
            # main bucket
            bucket_member(
                f'{kind}-service-account-standard-main-bucket-view-create',
                bucket=main_bucket.name,
                role=view_create_role.name,
                member=pulumi.Output.concat('serviceAccount:', service_account),
            )

            # main-tmp bucket
            bucket_member(
                f'{kind}-service-account-standard-main-tmp-bucket-view-create',
                bucket=main_tmp_bucket.name,
                role=view_create_role.name,
                member=pulumi.Output.concat('serviceAccount:', service_account),
            )

            # main-metadata bucket
            bucket_member(
                f'{kind}-service-account-standard-main-metadata-bucket-view-create',
                bucket=main_metadata_bucket.name,
                role=view_create_role.name,
                member=pulumi.Output.concat('serviceAccount:', service_account),
            )

            # main-web bucket
            bucket_member(
                f'{kind}-service-account-standard-main-web-bucket-view-create',
                bucket=main_web_bucket.name,
                role=view_create_role.name,
                member=pulumi.Output.concat('serviceAccount:', service_account),
            )

        if access_level == 'full':
            # main bucket
            bucket_member(
                f'{kind}-service-account-full-main-bucket-admin',
                bucket=main_bucket.name,
                role='roles/storage.admin',
                member=pulumi.Output.concat('serviceAccount:', service_account),
            )

            # main-tmp bucket
            bucket_member(
                f'{kind}-service-account-full-main-tmp-bucket-admin',
                bucket=main_tmp_bucket.name,
                role='roles/storage.admin',
                member=pulumi.Output.concat('serviceAccount:', service_account),
            )

            # main-metadata bucket
            bucket_member(
                f'{kind}-service-account-full-main-metadata-bucket-admin',
                bucket=main_metadata_bucket.name,
                role='roles/storage.admin',
                member=pulumi.Output.concat('serviceAccount:', service_account),
            )

            # main-web bucket
            bucket_member(
                f'{kind}-service-account-full-main-web-bucket-admin',
                bucket=main_web_bucket.name,
                role='roles/storage.admin',
                member=pulumi.Output.concat('serviceAccount:', service_account),
            )

            # upload bucket
            bucket_member(
                f'{kind}-service-account-full-upload-bucket-admin',
                bucket=upload_bucket.name,
                role='roles/storage.admin',
                member=pulumi.Output.concat('serviceAccount:', service_account),
            )

            # archive bucket
            bucket_member(
                f'{kind}-service-account-full-archive-bucket-admin',
                bucket=archive_bucket.name,
                role='roles/storage.admin',
                member=pulumi.Output.concat('serviceAccount:', service_account),
            )

            # release bucket
            if enable_release:
                bucket_member(
                    f'{kind}-service-account-full-release-bucket-admin',
                    bucket=release_bucket.name,
                    role='roles/storage.admin',
                    member=pulumi.Output.concat('serviceAccount:', service_account),
                )

    # Notebook permissions
    notebook_account = gcp.serviceaccount.Account(
        'notebook-account',
        project=NOTEBOOKS_PROJECT,
        account_id=f'notebook-{dataset}',
        display_name=f'Notebook service account for dataset {dataset}',
        opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
    )

    gcp.projects.IAMMember(
        'notebook-account-compute-admin',
        project=NOTEBOOKS_PROJECT,
        role='roles/compute.admin',
        member=pulumi.Output.concat('serviceAccount:', notebook_account.email),
    )

    gcp.serviceaccount.IAMMember(
        'notebook-account-users',
        service_account_id=notebook_account,
        role='roles/iam.serviceAccountUser',
        member=pulumi.Output.concat('group:', access_group.group_key.id),
    )

    # Grant the notebook account the same permissions as the access group members.
    gcp.cloudidentity.GroupMembership(
        'notebook-service-account-access-group-member',
        group=access_group.id,
        preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
            id=notebook_account.email
        ),
        roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
    )

    for kind, access_level, service_account in service_accounts:
        # To use a service account for VMs, accounts need to be allowed to act on their
        # own behalf ;).
        project = HAIL_PROJECT if kind == 'hail' else project_id
        gcp.serviceaccount.IAMMember(
            f'{kind}-service-account-{access_level}-service-account-user',
            service_account_id=pulumi.Output.concat(
                'projects/', project, '/serviceAccounts/', service_account
            ),
            role='roles/iam.serviceAccountUser',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

    # Allow Hail service accounts to start Dataproc clusters. That's only necessary
    # until Hail Query is feature complete.
    for access_level, service_account in hail_service_accounts:
        gcp.projects.IAMMember(
            f'hail-service-account-{access_level}-dataproc-admin',
            project=HAIL_PROJECT,
            role='roles/dataproc.admin',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

        gcp.projects.IAMMember(
            f'hail-service-account-{access_level}-dataproc-worker',
            project=HAIL_PROJECT,
            role='roles/dataproc.worker',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

        # Necessary for requester-pays buckets, e.g. to use VEP.
        gcp.projects.IAMMember(
            f'hail-service-account-{access_level}-serviceusage-consumer',
            project=HAIL_PROJECT,
            role='roles/serviceusage.serviceUsageConsumer',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

        # Add Hail service accounts to Cromwell access group.
        gcp.cloudidentity.GroupMembership(
            f'hail-service-account-{access_level}-cromwell-access',
            group=CROMWELL_ACCESS_GROUP_ID,
            preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
                id=service_account,
            ),
            roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
        )

    for access_level, service_account in cromwell_service_accounts:
        # Allow the Cromwell server to run worker VMs using the Cromwell service
        # accounts.
        gcp.serviceaccount.IAMMember(
            f'cromwell-runner-{access_level}-service-account-user',
            service_account_id=pulumi.Output.concat(
                'projects/', project_id, '/serviceAccounts/', service_account
            ),
            role='roles/iam.serviceAccountUser',
            member=f'serviceAccount:{CROMWELL_RUNNER_ACCOUNT}',
        )

        # Allow the Cromwell service accounts to run workflows.
        gcp.projects.IAMMember(
            f'cromwell-service-account-{access_level}-workflows-runner',
            role='roles/lifesciences.workflowsRunner',
            member=pulumi.Output.concat('serviceAccount:', service_account),
        )

        # Store the service account key as a secret that's readable by the
        # analysis-runner.
        key = gcp.serviceaccount.Key(
            f'cromwell-service-account-{access_level}-key',
            service_account_id=service_account,
        )

        secret = gcp.secretmanager.Secret(
            f'cromwell-service-account-{access_level}-secret',
            secret_id=f'{dataset}-cromwell-{access_level}-key',
            project=ANALYSIS_RUNNER_PROJECT,
            replication=gcp.secretmanager.SecretReplicationArgs(
                user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
                    replicas=[
                        gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                            location='australia-southeast1',
                        ),
                    ],
                ),
            ),
        )

        gcp.secretmanager.SecretVersion(
            f'cromwell-service-account-{access_level}-secret-version',
            secret=secret.id,
            secret_data=key.private_key.apply(
                lambda s: base64.b64decode(s).decode('utf-8')
            ),
        )

        gcp.secretmanager.SecretIamMember(
            f'cromwell-service-account-{access_level}-secret-accessor',
            project=ANALYSIS_RUNNER_PROJECT,
            secret_id=secret.id,
            role='roles/secretmanager.secretAccessor',
            member=f'serviceAccount:{ANALYSIS_RUNNER_SERVICE_ACCOUNT}',
        )


if __name__ == '__main__':
    main()
