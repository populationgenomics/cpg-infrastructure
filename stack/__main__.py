"""
Pulumi stack to set up buckets and permission groups
"""

import pulumi
import pulumi_gcp as gcp

DOMAIN = 'populationgenomics.org.au'
CUSTOMER_ID = 'C010ys3gt'
REGION = 'australia-southeast1'

# Fetch configuration.
config = pulumi.Config()
enable_release = config.get_bool('enable_release')
archive_age = config.get_int('archive_age') or 30
# The GSA email address associated with the Hail service account.
hail_service_account = config.require('hail_service_account')
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

# The Cloud Identity API is required for creating access groups.
cloudidentity = gcp.projects.Service(
    'cloudidentity-service',
    service='cloudidentity.googleapis.com',
    disable_on_destroy=False,
)

upload_account = gcp.serviceaccount.Account(
    'upload-service-account',
    account_id='upload',
    display_name='upload',
    opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
)

gcp.storage.BucketIAMMember(
    'upload-permissions-viewer',
    bucket=upload_bucket.name,
    role='roles/storage.objectViewer',
    member=pulumi.Output.concat('serviceAccount:', upload_account.email),
)

gcp.storage.BucketIAMMember(
    'upload-permissions-creator',
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


restricted_access_group = create_group(group_mail('restricted-access'))

listing_role = gcp.projects.IAMCustomRole(
    'storage-listing-role',
    description='Allows listing of storage objects',
    permissions=['storage.objects.list'],
    role_id='storageObjectLister',
    title='Storage Object Lister',
    opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
)

add_bucket_permissions(
    'restricted-access-main-lister',
    restricted_access_group,
    main_bucket,
    listing_role.name,
)

add_bucket_permissions(
    'restricted-access-analysis-viewer',
    restricted_access_group,
    analysis_bucket,
    'roles/storage.objectViewer',
)

add_bucket_permissions(
    'restricted-access-test-viewer',
    restricted_access_group,
    test_bucket,
    'roles/storage.objectViewer',
)

add_bucket_permissions(
    'restricted-access-temporary-admin',
    restricted_access_group,
    temporary_bucket,
    'roles/storage.objectAdmin',
)

extended_access_group = create_group(group_mail('extended-access'))

add_bucket_permissions(
    'extended-access-main-admin',
    extended_access_group,
    main_bucket,
    'roles/storage.objectAdmin',
)

add_bucket_permissions(
    'extended-access-analysis-admin',
    extended_access_group,
    analysis_bucket,
    'roles/storage.objectAdmin',
)

add_bucket_permissions(
    'extended-access-test-admin',
    extended_access_group,
    test_bucket,
    'roles/storage.objectAdmin',
)

add_bucket_permissions(
    'extended-access-temporary-admin',
    extended_access_group,
    temporary_bucket,
    'roles/storage.objectAdmin',
)

if enable_release:
    release_bucket = create_bucket(
        bucket_name('release-requester-pays'),
        lifecycle_rules=[undelete_rule],
        requester_pays=True,
    )

    add_bucket_permissions(
        'restricted-access-release-viewer',
        restricted_access_group,
        release_bucket,
        'roles/storage.objectViewer',
    )

    add_bucket_permissions(
        'extended-access-release-viewer',
        extended_access_group,
        release_bucket,
        'roles/storage.objectAdmin',
    )

    release_access_group = create_group(group_mail('release-access'))

    add_bucket_permissions(
        'release-access-release-viewer',
        release_access_group,
        release_bucket,
        'roles/storage.objectViewer',
    )

# Cloud Run is used for the server component of the analysis-runner.
cloudrun = gcp.projects.Service(
    'cloudrun-service', service='run.googleapis.com', disable_on_destroy=False
)

analysis_runner_service_account = gcp.serviceaccount.Account(
    'analysis-runner-service-account',
    account_id='analysis-runner-server',
    display_name='analysis-runner service account',
    opts=pulumi.resource.ResourceOptions(depends_on=[cloudidentity]),
)

secretmanager = gcp.projects.Service(
    'secretmanager-service',
    service='secretmanager.googleapis.com',
    disable_on_destroy=False,
)

# The analysis-runner needs access to two secrets: a list of allowed
# repositories and a Hail Batch service account token.
for secret_name in 'hail-token', 'allowed-repos':
    secret = gcp.secretmanager.Secret(
        f'{secret_name}-secret',
        replication=gcp.secretmanager.SecretReplicationArgs(
            user_managed=gcp.secretmanager.SecretReplicationUserManagedArgs(
                replicas=[
                    gcp.secretmanager.SecretReplicationUserManagedReplicaArgs(
                        location=REGION
                    ),
                ],
            ),
        ),
        secret_id=secret_name,
        opts=pulumi.resource.ResourceOptions(depends_on=[secretmanager]),
    )

    gcp.secretmanager.SecretIamMember(
        f'{secret_name}-secret-reader',
        secret_id=secret.id,
        role='roles/secretmanager.secretAccessor',
        member=pulumi.Output.concat(
            'serviceAccount:', analysis_runner_service_account.email
        ),
    )

# Allow the Cloud Run Service Agent and the Hail service account to pull images. Note
# that the global project will refer to the dataset, but the Docker image is stored in
# the "analysis-runner" project's Artifact Registry repository.
analysis_runner_repo = gcp.artifactregistry.RepositoryIamMember(
    'analysis-runner-repo',
    project='analysis-runner',
    location=REGION,
    repository='images',
    role='roles/artifactregistry.reader',
    member=(
        f'serviceAccount:service-{project_number}'
        f'@serverless-robot-prod.iam.gserviceaccount.com'
    ),
    opts=pulumi.resource.ResourceOptions(depends_on=[cloudrun]),
)

gcp.artifactregistry.RepositoryIamMember(
    'hail-service-account-repo',
    project='analysis-runner',
    location=REGION,
    repository='images',
    role='roles/artifactregistry.reader',
    member=f'serviceAccount:{hail_service_account}',
)

analysis_runner_server = gcp.cloudrun.Service(
    'analysis-runner-server',
    location=REGION,
    autogenerate_revision_name=True,
    template=gcp.cloudrun.ServiceTemplateArgs(
        spec=gcp.cloudrun.ServiceTemplateSpecArgs(
            containers=[
                gcp.cloudrun.ServiceTemplateSpecContainerArgs(
                    envs=[
                        {'name': 'GCP_PROJECT', 'value': project_id},
                        {'name': 'DATASET', 'value': dataset},
                    ],
                    image=(
                        f'australia-southeast1-docker.pkg.dev/analysis-runner/'
                        f'images/server:78d20393125b'
                    ),
                )
            ],
            service_account_name=analysis_runner_service_account.email,
        ),
    ),
    opts=pulumi.resource.ResourceOptions(depends_on=[analysis_runner_repo]),
)

pulumi.export('analysis-runner-server URL', analysis_runner_server.statuses[0].url)

# Restrict Cloud Run invokers to the restricted access group.
gcp.cloudrun.IamMember(
    'analysis-runner-invoker',
    service=analysis_runner_server.name,
    location=REGION,
    role='roles/run.invoker',
    member=pulumi.Output.concat('group:', restricted_access_group.group_key.id),
)

# The bucket used for Hail Batch pipelines.
hail_bucket = create_bucket(bucket_name('hail'), lifecycle_rules=[undelete_rule])

gcp.storage.BucketIAMMember(
    'hail-bucket-permissions',
    bucket=hail_bucket.name,
    role='roles/storage.objectAdmin',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account),
)

# The Hail service account has creator permissions for all buckets.
# Archive and upload buckets are handled by the upload processor.
gcp.storage.BucketIAMMember(
    'hail-main-creator',
    bucket=main_bucket.name,
    role='roles/storage.objectCreator',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account),
)

gcp.storage.BucketIAMMember(
    'hail-analysis-creator',
    bucket=analysis_bucket.name,
    role='roles/storage.objectCreator',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account),
)

gcp.storage.BucketIAMMember(
    'hail-test-creator',
    bucket=test_bucket.name,
    role='roles/storage.objectCreator',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account),
)

gcp.storage.BucketIAMMember(
    'hail-temporary-creator',
    bucket=temporary_bucket.name,
    role='roles/storage.objectCreator',
    member=pulumi.Output.concat('serviceAccount:', hail_service_account),
)

if enable_release:
    gcp.storage.BucketIAMMember(
        'hail-release-creator',
        bucket=release_bucket.name,
        role='roles/storage.objectCreator',
        member=pulumi.Output.concat('serviceAccount:', hail_service_account),
    )
