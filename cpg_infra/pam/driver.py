# flake8: noqa: ANN204,ANN401

"""PAM Infrastructure Driver

Core logic for setting up Privileged Access Manager (PAM) resources:
- PAM entitlements for storage bucket access
- GitHub Workload Identity Federation pools
- WIF bindings and token creator permissions
"""

from pathlib import Path

import pulumi
import pulumi_gcp as gcp
import yaml


class PAMInfra:
    """Helper for generating consistent Pulumi resource names for PAM resources"""

    def __init__(self, dataset_name: str, project_id: str):
        self.dataset_name = dataset_name
        self.project_id = project_id

    def get_pulumi_name(self, name: str) -> str:
        """Get dataset-prefixed Pulumi resource name"""
        return f'{self.dataset_name}-{name}'


def get_project_number(project_id: str) -> str:
    """
    Get GCP project number from project ID

    Args:
        project_id: GCP project ID

    Returns:
        Project number as string
    """
    project = gcp.organizations.get_project(project_id=project_id)
    return project.number


def load_pam_config():
    """Load PAM configuration from YAML"""
    config_path = Path(__file__).parent / 'pam.yaml'
    if not config_path.exists():
        return {}
    with open(config_path, encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def create_pam_entitlement(
    resource_name: str,
    dataset: str,
    bucket_name: str,
    role: str,
    access_type: str,  # 'read' or 'write'
    project_number: str,
    location: str,
    principals: list[str] | pulumi.Output[list[str]],
    max_duration: str = '604800s',
):
    """
    Create a PAM entitlement for storage bucket access

    Args:
        resource_name: Pulumi resource name
        dataset: Dataset name
        bucket_name: GCS bucket name (e.g., 'cpg-test-dataset-main')
        role: IAM role to grant (e.g., 'roles/storage.objectViewer')
        access_type: 'read' or 'write' for entitlement naming
        project_number: Numeric GCP project ID
        location: PAM location (typically 'global')
        principals: List of principals (e.g., ['serviceAccount:sa@project.iam.gserviceaccount.com',
                                                'user:person@example.com',
                                                'group:team@example.com'])
                   Can be a list or Pulumi Output[list]
        max_duration: Maximum grant duration (default 7 days)

    Returns:
        PAM Entitlement resource
    """
    entitlement_id = f'pam-{dataset}-{access_type}'

    # IAM condition for bucket access (both bucket-level and object-level operations)
    condition_expr = f"""resource.service == "storage.googleapis.com" &&
(
  resource.name == "projects/_/buckets/{bucket_name}" ||
  resource.name.startsWith("projects/_/buckets/{bucket_name}/objects/")
)"""

    return gcp.privilegedaccessmanager.Entitlement(
        resource_name,
        parent=f'projects/{project_number}/locations/{location}',
        entitlement_id=entitlement_id,
        max_request_duration=max_duration,
        location='global',
        # Requester justification config
        requester_justification_config=gcp.privilegedaccessmanager.EntitlementRequesterJustificationConfigArgs(
            unstructured=gcp.privilegedaccessmanager.EntitlementRequesterJustificationConfigUnstructuredArgs(),
        ),
        # Eligible users/service accounts/groups
        eligible_users=[
            gcp.privilegedaccessmanager.EntitlementEligibleUserArgs(
                principals=principals
                if isinstance(principals, pulumi.Output)
                else pulumi.Output.from_input(principals),
            ),
        ],
        # Privileged access with IAM condition
        privileged_access=gcp.privilegedaccessmanager.EntitlementPrivilegedAccessArgs(
            gcp_iam_access=gcp.privilegedaccessmanager.EntitlementPrivilegedAccessGcpIamAccessArgs(
                resource_type='cloudresourcemanager.googleapis.com/Project',
                resource=f'//cloudresourcemanager.googleapis.com/projects/{project_number}',
                role_bindings=[
                    gcp.privilegedaccessmanager.EntitlementPrivilegedAccessGcpIamAccessRoleBindingArgs(
                        role=role,
                        condition_expression=condition_expr,
                    ),
                ],
            ),
        ),
        # Auto-approval (empty steps)
        approval_workflow=gcp.privilegedaccessmanager.EntitlementApprovalWorkflowArgs(
            manual_approvals=gcp.privilegedaccessmanager.EntitlementApprovalWorkflowManualApprovalsArgs(
                require_approver_justification=False,
                steps=[],
            ),
        ),
    )


def setup_pam_entitlements(
    infra: PAMInfra,
    dataset: str,
    project_number: str,
    principals: list[str] | pulumi.Output[list[str]],
):
    """
    Setup PAM entitlements for the given principals

    Args:
        infra: GCP infrastructure instance (for get_pulumi_name)
        dataset: Dataset name
        project_number: Numeric GCP project ID
        principals: List of principals (can include notebook SA, users, groups)

    Returns:
        Dictionary mapping access_type to entitlement_id (e.g., {'read': 'pam-dataset-read'})
    """
    # Auto-discover main bucket name from convention
    main_bucket_name = f'cpg-{dataset}-main'

    entitlement_ids = {}

    # Create read entitlement for main bucket
    _read_entitlement = create_pam_entitlement(
        resource_name=infra.get_pulumi_name('pam-entitlement-main-read'),
        dataset=dataset,
        bucket_name=main_bucket_name,
        role='roles/storage.objectViewer',
        access_type='read',
        project_number=project_number,
        location='global',
        principals=principals,
        max_duration='604800s',  # 7 days
    )

    entitlement_ids['read'] = f'pam-{dataset}-read'

    # Return entitlement IDs for export
    return entitlement_ids


def setup_github_wif_pool(
    infra: PAMInfra,
    project_id: str,
    project_number: str,
    broker_sa_name: str | pulumi.Output[str],
    wif_github_repository: str,
    wif_github_environment: str,
):
    """
    Setup GitHub Workload Identity Federation pool and provider

    Args:
        infra: Infrastructure instance (for get_pulumi_name)
        project_id: GCP project ID
        project_number: Numeric GCP project ID
        broker_sa_name: PAM broker service account name
        wif_github_repository: GitHub repository (e.g., 'org/repo')
        wif_github_environment: GitHub environment name for binding
    """
    # Extract GitHub org from repository
    github_org = wif_github_repository.split('/')[0]

    # Constants
    wif_pool_name = 'github-pool'
    wif_provider_name = 'github-provider'

    # Create Workload Identity Pool
    wif_pool = gcp.iam.WorkloadIdentityPool(
        infra.get_pulumi_name('github-wif-pool'),
        workload_identity_pool_id=wif_pool_name,
        project=project_id,
        display_name='GitHub Actions Pool',
        description='Workload Identity Pool for GitHub Actions OIDC',
        disabled=False,
        opts=pulumi.ResourceOptions(
            protect=True,
        ),
    )

    # Create OIDC Provider
    wif_provider = gcp.iam.WorkloadIdentityPoolProvider(
        infra.get_pulumi_name('github-wif-provider'),
        workload_identity_pool_id=wif_pool.workload_identity_pool_id,
        workload_identity_pool_provider_id=wif_provider_name,
        project=project_id,
        display_name='GitHub Actions Provider',
        description=f'OIDC provider for {github_org} GitHub repositories',
        disabled=False,
        attribute_mapping={
            'google.subject': 'assertion.sub',
            'attribute.actor': 'assertion.actor',
            'attribute.repository': 'assertion.repository',
        },
        attribute_condition=f"assertion.repository_owner == '{github_org}'",
        oidc=gcp.iam.WorkloadIdentityPoolProviderOidcArgs(
            issuer_uri='https://token.actions.githubusercontent.com',
        ),
        opts=pulumi.ResourceOptions(
            depends_on=[wif_pool],
            protect=True,
        ),
    )

    # Bind broker SA to WIF pool for the specified environment
    principal = pulumi.Output.all(
        project_number=project_number,
        environment=wif_github_environment,
        repository=wif_github_repository,
        pool_name=wif_pool_name,
    ).apply(
        lambda args: (
            f"principal://iam.googleapis.com/projects/{args['project_number']}/"
            f"locations/global/workloadIdentityPools/{args['pool_name']}/"
            f"subject/repo:{args['repository']}:environment:{args['environment']}"
        )
    )

    gcp.serviceaccount.IAMBinding(
        infra.get_pulumi_name('pam-broker-wif-binding'),
        service_account_id=broker_sa_name,
        role='roles/iam.workloadIdentityUser',
        members=[principal],
        opts=pulumi.ResourceOptions(
            depends_on=[wif_provider],
        ),
    )
