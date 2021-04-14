"""Pulumi stack to set up buckets and permission groups."""

import pulumi
import pulumi_gcp as gcp

DOMAIN = 'populationgenomics.org.au'
CUSTOMER_ID = 'C010ys3gt'
REGION = 'australia-southeast1'
ANALYSIS_RUNNER_PROJECT = 'analysis-runner'
ANALYSIS_RUNNER_SERVICE_ACCOUNT = (
    'analysis-runner-server@analysis-runner.iam.gserviceaccount.com'
)
REFERENCE_BUCKET_NAME = 'cpg-reference'

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


undelete_rule = gcp.storage.BucketLifecycleRuleArgs(
    action=gcp.storage.BucketLifecycleRuleActionArgs(type='Delete'),
    condition=gcp.storage.BucketLifecycleRuleConditionArgs(
        age=30, with_state='ARCHIVED'
    ),
)

upload_bucket = create_bucket(bucket_name('upload'), lifecycle_rules=[undelete_rule])

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

gcp.storage.BucketIAMMember(
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

main_bucket = create_bucket(bucket_name('main'), lifecycle_rules=[undelete_rule])

analysis_bucket = create_bucket(
    bucket_name('analysis'), lifecycle_rules=[undelete_rule]
)

test_bucket = create_bucket(bucket_name('test'), lifecycle_rules=[undelete_rule])

temporary_bucket = create_bucket(
    bucket_name('temporary'),
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
    return gcp.storage.BucketIAMMember(
        name,
        bucket=bucket.name,
        role=role,
        member=pulumi.Output.concat('group:', group.group_key.id),
    )


access_group = create_group(group_mail('access'))

listing_role = gcp.projects.IAMCustomRole(
    'storage-listing-role',
    description='Allows listing of storage objects',
    permissions=['storage.objects.list'],
    role_id='storageObjectLister',
    title='Storage Object Lister',
    opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
)

add_bucket_permissions(
    'access-group-test-bucket-viewer',
    access_group,
    test_bucket,
    'roles/storage.objectViewer',
)

add_bucket_permissions(
    'access-group-temporary-bucket-admin',
    access_group,
    temporary_bucket,
    'roles/storage.admin',
)

add_bucket_permissions(
    'access-group-main-bucket-lister',
    access_group,
    main_bucket,
    listing_role.name,
)

add_bucket_permissions(
    'access-group-analysis-bucket-viewer',
    access_group,
    analysis_bucket,
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

# Allow the analysis-runner to check memberships.
gcp.cloudidentity.GroupMembership(
    'analysis-runner-restricted-member',
    group=access_group.id,
    preferred_member_key=gcp.cloudidentity.GroupMembershipPreferredMemberKeyArgs(
        id=ANALYSIS_RUNNER_SERVICE_ACCOUNT
    ),
    roles=[gcp.cloudidentity.GroupMembershipRoleArgs(name='MEMBER')],
)

# Allow the Hail service accounts to pull images. Note that the global project will
# refer to the dataset, but the Docker image is stored in the "analysis-runner"
# project's Artifact Registry repository.
gcp.artifactregistry.RepositoryIamMember(
    'hail-service-account-test-images-reader',
    project=ANALYSIS_RUNNER_PROJECT,
    location=REGION,
    repository='images',
    role='roles/artifactregistry.reader',
    member=f'serviceAccount:{hail_service_account_test}',
)

gcp.artifactregistry.RepositoryIamMember(
    'hail-service-account-standard-images-reader',
    project=ANALYSIS_RUNNER_PROJECT,
    location=REGION,
    repository='images',
    role='roles/artifactregistry.reader',
    member=f'serviceAccount:{hail_service_account_standard}',
)

gcp.artifactregistry.RepositoryIamMember(
    'hail-service-account-full-images-reader',
    project=ANALYSIS_RUNNER_PROJECT,
    location=REGION,
    repository='images',
    role='roles/artifactregistry.reader',
    member=f'serviceAccount:{hail_service_account_full}',
)

# The bucket used for Hail Batch pipelines.
hail_bucket = create_bucket(bucket_name('hail'), lifecycle_rules=[undelete_rule])

gcp.storage.BucketIAMMember(
    'hail-service-account-test-hail-bucket-admin',
    bucket=hail_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_test),
)

gcp.storage.BucketIAMMember(
    'hail-service-account-standard-hail-bucket-admin',
    bucket=hail_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

gcp.storage.BucketIAMMember(
    'hail-service-account-full-hail-bucket-admin',
    bucket=hail_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# Allow access to reference data.
gcp.storage.BucketIAMMember(
    'hail-service-account-test-reference-bucket-viewer',
    bucket=REFERENCE_BUCKET_NAME,
    role='roles/storage.objectViewer',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_test),
)

gcp.storage.BucketIAMMember(
    'hail-service-account-standard-reference-bucket-viewer',
    bucket=REFERENCE_BUCKET_NAME,
    role='roles/storage.objectViewer',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

gcp.storage.BucketIAMMember(
    'hail-service-account-full-reference-bucket-viewer',
    bucket=REFERENCE_BUCKET_NAME,
    role='roles/storage.objectViewer',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# Permissions increase by access level:
# - test: read test, write temporary
# - standard: read main, write analysis
# - full: write anywhere

# test bucket
gcp.storage.BucketIAMMember(
    'hail-service-account-test-test-bucket-viewer',
    bucket=test_bucket.name,
    role='roles/storage.objectViewer',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_test),
)

gcp.storage.BucketIAMMember(
    'hail-service-account-standard-test-bucket-viewer',
    bucket=test_bucket.name,
    role='roles/storage.objectViewer',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

gcp.storage.BucketIAMMember(
    'hail-service-account-full-test-bucket-admin',
    bucket=test_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# temporary bucket
gcp.storage.BucketIAMMember(
    'hail-service-account-test-temporary-bucket-admin',
    bucket=temporary_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_test),
)

gcp.storage.BucketIAMMember(
    'hail-service-account-standard-temporary-bucket-admin',
    bucket=temporary_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

gcp.storage.BucketIAMMember(
    'hail-service-account-full-temporary-bucket-admin',
    bucket=temporary_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# main bucket
gcp.storage.BucketIAMMember(
    'hail-service-account-standard-main-bucket-viewer',
    bucket=main_bucket.name,
    role='roles/storage.objectViewer',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

gcp.storage.BucketIAMMember(
    'hail-service-account-full-main-bucket-admin',
    bucket=main_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# analysis bucket
gcp.storage.BucketIAMMember(
    'hail-service-account-standard-analysis-bucket-viewer',
    bucket=analysis_bucket.name,
    role='roles/storage.objectViewer',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

gcp.storage.BucketIAMMember(
    'hail-service-account-standard-analysis-bucket-creator',
    bucket=analysis_bucket.name,
    role='roles/storage.objectCreator',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_standard),
)

gcp.storage.BucketIAMMember(
    'hail-service-account-full-analysis-bucket-admin',
    bucket=analysis_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# upload bucket
gcp.storage.BucketIAMMember(
    'hail-service-account-full-upload-bucket-admin',
    bucket=upload_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

# archive bucket
gcp.storage.BucketIAMMember(
    'hail-service-account-full-archive-bucket-admin',
    bucket=archive_bucket.name,
    role='roles/storage.admin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
)

if enable_release:
    gcp.storage.BucketIAMMember(
        'hail-service-account-full-release-bucket-admin',
        bucket=release_bucket.name,
        role='roles/storage.admin',
        member=pulumi.Output.concat('serviceAccount:', hail_service_account_full),
    )
