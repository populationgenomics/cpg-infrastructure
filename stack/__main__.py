"""Pulumi stack to set up buckets and permission groups."""

import pulumi
import pulumi_gcp as gcp

DOMAIN = 'populationgenomics.org.au'
CUSTOMER_ID = 'C010ys3gt'
REGION = 'australia-southeast1'
ANALYSIS_RUNNER_PROJECT = 'analysis-runner'
CPG_COMMON_PROJECT = 'cpg-common'
ANALYSIS_RUNNER_SERVICE_ACCOUNT = (
    'analysis-runner-server@analysis-runner.iam.gserviceaccount.com'
)
WEB_SERVER_SERVICE_ACCOUNT = 'web-server@analysis-runner.iam.gserviceaccount.com'
REFERENCE_BUCKET_NAME = 'cpg-reference'
NOTEBOOKS_PROJECT = 'notebooks-314505'
# cromwell-submission-access@populationgenomics.org.au
CROMWELL_ACCESS_GROUP_ID = 'groups/03cqmetx2922fyu'

# Fetch configuration.
config = pulumi.Config()
enable_release = config.get_bool('enable_release')
archive_age = config.get_int('archive_age') or 30
# The Hail service account email addresses associated with the three access levels.
hail_service_account_test = config.require('hail_service_account_test')
hail_service_account_standard = config.require('hail_service_account_standard')
hail_service_account_full = config.require('hail_service_account_full')
dataset = pulumi.get_stack()

project_id = gcp.organizations.get_project().project_id
project_number = gcp.organizations.get_project().number


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

# The Cloud Resource Manager API is required for the Cloud Identity API.
cloudresourcemanager = gcp.projects.Service(
    'cloudresourcemanager-service',
    service='cloudresourcemanager.googleapis.com',
    disable_on_destroy=False,
)

# The Cloud Identity API is required for creating access groups.
cloudidentity = gcp.projects.Service(
    'cloudidentity-service',
    service='cloudidentity.googleapis.com',
    disable_on_destroy=False,
    opts=pulumi.resource.ResourceOptions(depends_on=[cloudresourcemanager]),
)

upload_account = gcp.serviceaccount.Account(
    'upload-service-account',
    account_id='upload',
    display_name='upload',
    opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
)

upload_bucket = create_bucket(bucket_name('upload'), lifecycle_rules=[undelete_rule])

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

listing_role = gcp.projects.IAMCustomRole(
    'storage-listing-role',
    description='Allows listing of storage objects',
    permissions=['storage.objects.list', 'storage.buckets.list', 'storage.buckets.get'],
    role_id='storageObjectLister',
    title='Storage Object Lister',
    opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
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
    'access-group-main-bucket-lister',
    access_group,
    main_bucket,
    listing_role.name,
)

add_bucket_permissions(
    'access-group-main-tmp-bucket-lister',
    access_group,
    main_tmp_bucket,
    listing_role.name,
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

add_bucket_permissions(
    'access-group-upload-bucket-lister',
    access_group,
    upload_bucket,
    listing_role.name,
)

add_bucket_permissions(
    'access-group-archive-bucket-lister',
    access_group,
    archive_bucket,
    listing_role.name,
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

# Allow the analysis-runner to check memberships.
gcp.cloudidentity.GroupMembership(
    'analysis-runner-restricted-member',
    group=access_group.id,
    preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
        id=ANALYSIS_RUNNER_SERVICE_ACCOUNT
    ),
    roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
)

# Allow the web server to check memberships.
gcp.cloudidentity.GroupMembership(
    'web-server-restricted-member',
    group=access_group.id,
    preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
        id=WEB_SERVER_SERVICE_ACCOUNT
    ),
    roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
)

# Allow the Hail service accounts to pull images. Note that the global project will
# refer to the dataset, but the Docker images are stored in the "analysis-runner"
# and "cpg-common" projects' Artifact Registry repositories.
for access_level, service_account in [
    ('test', hail_service_account_test),
    ('standard', hail_service_account_standard),
    ('full', hail_service_account_full),
]:
    for project in [ANALYSIS_RUNNER_PROJECT, CPG_COMMON_PROJECT]:
        gcp.artifactregistry.RepositoryIamMember(
            f'hail-service-account-{access_level}-images-reader-in-{project}',
            project=project,
            location=REGION,
            repository='images',
            role='roles/artifactregistry.reader',
            member=f'serviceAccount:{service_account}',
        )

# The bucket used for Hail Batch pipelines.
hail_bucket = create_bucket(bucket_name('hail'), lifecycle_rules=[undelete_rule])

bucket_member(
    'hail-service-account-test-hail-bucket-admin',
    bucket=hail_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_test),
)

bucket_member(
    'hail-service-account-standard-hail-bucket-admin',
    bucket=hail_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

bucket_member(
    'hail-service-account-full-hail-bucket-admin',
    bucket=hail_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# Allow access to reference data.
bucket_member(
    'hail-service-account-test-reference-bucket-viewer',
    bucket=REFERENCE_BUCKET_NAME,
    role='roles/storage.objectViewer',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_test),
)

bucket_member(
    'hail-service-account-standard-reference-bucket-viewer',
    bucket=REFERENCE_BUCKET_NAME,
    role='roles/storage.objectViewer',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

bucket_member(
    'hail-service-account-full-reference-bucket-viewer',
    bucket=REFERENCE_BUCKET_NAME,
    role='roles/storage.objectViewer',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# Permissions increase by access level:
# - test: view / create on any "test" bucket
# - standard: view / create on any "test" or "main" bucket
# - full: view / create / delete anywhere

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

# test bucket
bucket_member(
    'hail-service-account-test-test-bucket-admin',
    bucket=test_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_test),
)

bucket_member(
    'hail-service-account-standard-test-bucket-admin',
    bucket=test_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

bucket_member(
    'hail-service-account-full-test-bucket-admin',
    bucket=test_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# test-tmp bucket
bucket_member(
    'hail-service-account-test-test-tmp-bucket-admin',
    bucket=test_tmp_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_test),
)

bucket_member(
    'hail-service-account-standard-test-tmp-bucket-admin',
    bucket=test_tmp_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

bucket_member(
    'hail-service-account-full-test-tmp-bucket-admin',
    bucket=test_tmp_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# test-metadata bucket
bucket_member(
    'hail-service-account-test-test-metadata-bucket-admin',
    bucket=test_metadata_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_test),
)

bucket_member(
    'hail-service-account-standard-test-metadata-bucket-admin',
    bucket=test_metadata_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

bucket_member(
    'hail-service-account-full-test-metadata-bucket-admin',
    bucket=test_metadata_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# test-web bucket
bucket_member(
    'hail-service-account-test-test-web-bucket-admin',
    bucket=test_web_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_test),
)

bucket_member(
    'hail-service-account-standard-test-web-bucket-admin',
    bucket=test_web_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

bucket_member(
    'hail-service-account-full-test-web-bucket-admin',
    bucket=test_web_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# main bucket
bucket_member(
    'hail-service-account-standard-main-bucket-view-create',
    bucket=main_bucket.name,
    role=view_create_role.name,
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

bucket_member(
    'hail-service-account-full-main-bucket-admin',
    bucket=main_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# main-tmp bucket
bucket_member(
    'hail-service-account-standard-main-tmp-bucket-view-create',
    bucket=main_tmp_bucket.name,
    role=view_create_role.name,
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

bucket_member(
    'hail-service-account-full-main-tmp-bucket-admin',
    bucket=main_tmp_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# main-metadata bucket
bucket_member(
    'hail-service-account-standard-main-metadata-bucket-view-create',
    bucket=main_metadata_bucket.name,
    role=view_create_role.name,
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

bucket_member(
    'hail-service-account-full-main-metadata-bucket-admin',
    bucket=main_metadata_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# main-web bucket
bucket_member(
    'hail-service-account-standard-main-web-bucket-view-create',
    bucket=main_web_bucket.name,
    role=view_create_role.name,
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

bucket_member(
    'hail-service-account-full-main-web-bucket-admin',
    bucket=main_web_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# upload bucket
bucket_member(
    'hail-service-account-full-upload-bucket-admin',
    bucket=upload_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# archive bucket
bucket_member(
    'hail-service-account-full-archive-bucket-admin',
    bucket=archive_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

if enable_release:
    bucket_member(
        'hail-service-account-full-release-bucket-admin',
        bucket=release_bucket.name,
        role='roles/storage.admin',
        member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
    )

# Notebook permissions
notebook_account = gcp.serviceaccount.Account(
    'notebook-account',
    project=NOTEBOOKS_PROJECT,
    account_id=f'notebook-{dataset}',
    display_name=f'Notebook service account for dataset {dataset}',
    opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
)

gcp.projects.IAMBinding(
    'notebook-account-compute-admin',
    project=NOTEBOOKS_PROJECT,
    role='roles/compute.admin',
    members=[pulumi.Output.concat('serviceAccount:', notebook_account.email)],
)

gcp.serviceaccount.IAMBinding(
    'notebook-account-users',
    service_account_id=notebook_account,
    role='roles/iam.serviceAccountUser',
    members=[pulumi.Output.concat('group:', access_group.group_key.id)],
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

# Add Hail service accounts to Cromwell access group.
gcp.cloudidentity.GroupMembership(
    'hail-service-account-test-cromwell-access',
    group=CROMWELL_ACCESS_GROUP_ID,
    preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
        id=hail_service_account_test,
    ),
    roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
)

gcp.cloudidentity.GroupMembership(
    'hail-service-account-standard-cromwell-access',
    group=CROMWELL_ACCESS_GROUP_ID,
    preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
        id=hail_service_account_standard,
    ),
    roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
)

gcp.cloudidentity.GroupMembership(
    'hail-service-account-full-cromwell-access',
    group=CROMWELL_ACCESS_GROUP_ID,
    preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
        id=hail_service_account_full,
    ),
    roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
)
