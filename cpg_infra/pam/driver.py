# flake8: noqa: ANN204

"""PAM Infrastructure Driver

Core logic for setting up Privileged Access Manager (PAM) resources:
- PAM entitlements for storage bucket access
- Token creator role bindings
"""

import pulumi
import pulumi_gcp as gcp
import pulumi_github as github


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


def create_broker_service_account(
    project_id: str,
    account_id: str = 'pam-broker',
    display_name: str = 'PAM Broker Service Account',
) -> gcp.serviceaccount.Account:
    """
    Create PAM broker service account

    Args:
        project_id: GCP project ID where broker SA will be created
        account_id: Service account ID (defaults to 'pam-broker')
        display_name: Display name for the service account

    Returns:
        Service account resource
    """
    return gcp.serviceaccount.Account(
        'pam-broker',
        account_id=account_id,
        project=project_id,
        display_name=display_name,
        description=(
            'Broker SA for GitHub Actions PAM integration. '
            'Authenticates via WIF and impersonates dataset notebook SAs '
            'to request temporary PAM grants for storage access.'
        ),
    )


def setup_github_environment_secrets(
    wif_repository: str,
    wif_environment: str,
    wif_provider_path: str | pulumi.Output[str],
    broker_sa_email: str | pulumi.Output[str],
    common_project_number: str | pulumi.Output[str],
    common_project_id: str,
):
    """
    Setup GitHub environment and secrets for PAM workflow

    Creates GitHub environment and populates it with secrets needed
    for PAM grant requests via GitHub Actions.

    Args:
        wif_repository: GitHub repository (e.g., 'org/repo')
        wif_environment: GitHub environment name
        wif_provider_path: Full WIF provider resource path
        broker_sa_email: PAM broker service account email
        common_project_number: GCP project number (where WIF pool lives)
        common_project_id: GCP project ID (where WIF pool lives)
    """
    # Extract repo name from org/repo format
    repo_name = wif_repository.split('/')[-1]

    # Create PAM environment in GitHub repository
    # Note: This requires a GITHUB_TOKEN with appropriate permissions
    pam_env = github.RepositoryEnvironment(
        'pam-environment',
        repository=repo_name,
        environment=wif_environment,
    )

    # Create environment secrets
    github.ActionsEnvironmentSecret(
        'wif-provider-secret',
        repository=repo_name,
        environment=wif_environment,
        secret_name='WIF_PROVIDER',  # noqa: S106
        plaintext_value=wif_provider_path,
        opts=pulumi.ResourceOptions(depends_on=[pam_env]),
    )

    github.ActionsEnvironmentSecret(
        'pam-broker-sa-secret',
        repository=repo_name,
        environment=wif_environment,
        secret_name='PAM_BROKER_SA',  # noqa: S106
        plaintext_value=broker_sa_email,
        opts=pulumi.ResourceOptions(depends_on=[pam_env]),
    )

    github.ActionsEnvironmentSecret(
        'gcp-project-number-secret',
        repository=repo_name,
        environment=wif_environment,
        secret_name='GCP_PROJECT_NUMBER',  # noqa: S106
        plaintext_value=str(common_project_number)
        if isinstance(common_project_number, str)
        else common_project_number.apply(str),
        opts=pulumi.ResourceOptions(depends_on=[pam_env]),
    )

    github.ActionsEnvironmentSecret(
        'gcp-common-project-secret',
        repository=repo_name,
        environment=wif_environment,
        secret_name='GCP_COMMON_PROJECT',  # noqa: S106
        plaintext_value=common_project_id,
        opts=pulumi.ResourceOptions(depends_on=[pam_env]),
    )

    pulumi.log.info(
        f'Created GitHub environment "{wif_environment}" '
        f'with secrets for repository {wif_repository}',
    )


def grant_token_creator_to_broker(
    dataset_name: str,
    notebook_sa_id: str | pulumi.Output[str],
    broker_sa_email: str | pulumi.Output[str],
):
    """
    Grant token creator role to broker SA on notebook SA

    This allows the broker (authenticated via WIF) to impersonate
    the notebook SA for PAM grant requests.

    Args:
        dataset_name: Dataset name (for resource naming)
        notebook_sa_id: Full resource ID of notebook service account
        broker_sa_email: Email of PAM broker service account
    """
    return gcp.serviceaccount.IAMMember(
        f'pam-tc-{dataset_name}',  # Shortened prefix to avoid length limits
        service_account_id=notebook_sa_id,
        role='roles/iam.serviceAccountTokenCreator',
        member=pulumi.Output.concat(
            'serviceAccount:',
            broker_sa_email,
        ),
    )
