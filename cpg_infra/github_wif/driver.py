# flake8: noqa: ANN001,ANN201,ERA001
"""
GitHub Workload Identity Federation (WIF) setup for Pulumi

This module provides functionality to set up GitHub repositories with
GCP Workload Identity Federation for OIDC authentication and Artifact Registry access.
"""

import re
from typing import Any, Literal

import pulumi
import pulumi_gcp as gcp

# Constants
WIF_POOL_NAME = 'github-pool'
WIF_PROVIDER_NAME = 'github-provider'
GITHUB_ORG = 'populationgenomics'

# Service account name max length is 30 characters
SA_NAME_MAX_LENGTH = 30


def validate_github_wif_config(config: dict[str, Any]) -> None:
    """
    Validate GitHub WIF configuration structure.

    Args:
        config: Parsed configuration dictionary

    Raises:
        ValueError: If configuration is invalid
    """
    if not config or 'projects' not in config:
        raise ValueError('Invalid GitHub WIF config: missing "projects" key')

    for project_id, project_config in config['projects'].items():
        if 'project_number' not in project_config:
            raise ValueError(f'Project {project_id} missing "project_number"')
        if 'location' not in project_config:
            raise ValueError(f'Project {project_id} missing "location"')
        if 'repositories' not in project_config:
            raise ValueError(f'Project {project_id} missing "repositories"')


def sanitize_sa_name(
    repo_name: str, environment: str, max_length: int = SA_NAME_MAX_LENGTH
) -> str:
    """
    Create a service account name from repo and environment, handling length limits.

    GCP service account IDs must be:
    - 6-30 characters
    - Lowercase letters, digits, hyphens
    - Start with lowercase letter

    Args:
        repo_name: GitHub repository name
        environment: Environment name (e.g., 'development', 'production')
        max_length: Maximum length for service account name

    Returns:
        Sanitized service account name
    """
    # Remove any non-alphanumeric characters and convert to lowercase
    clean_repo = re.sub(r'[^a-z0-9-]', '-', repo_name.lower())
    clean_env = re.sub(r'[^a-z0-9-]', '-', environment.lower())

    # Shorten environment name if needed
    env_short = clean_env
    if clean_env == 'development':
        env_short = 'dev'
    elif clean_env == 'production':
        env_short = 'prod'

    # Try full name first: {repo}-img-{env}-deployer
    sa_name = f'{clean_repo}-img-{env_short}-deployer'

    # If too long, truncate repo name
    if len(sa_name) > max_length:
        # Pattern: {repo}-img-{env}-deployer
        # Fixed parts: '-img-' (5 chars) + '-deployer' (9 chars) = 14 chars
        # Variable part: env_short
        fixed_overhead = len('-img-') + len('-deployer')  # 14
        available_for_repo = max_length - len(env_short) - fixed_overhead
        clean_repo = clean_repo[:available_for_repo]
        sa_name = f'{clean_repo}-img-{env_short}-deployer'

    # Ensure it starts with a letter
    if sa_name[0].isdigit():
        sa_name = 'gh-' + sa_name[3:]  # Replace first 3 chars with 'gh-'

    return sa_name


def get_wif_provider_resource_name(project_number: str) -> pulumi.Output[str]:
    """
    Get the full resource name for the WIF provider.

    Args:
        project_number: GCP project number

    Returns:
        Full WIF provider resource name
    """
    return pulumi.Output.concat(
        f'projects/{project_number}/locations/global/',
        f'workloadIdentityPools/{WIF_POOL_NAME}/',
        f'providers/{WIF_PROVIDER_NAME}',
    )


def check_or_create_wif_pool(project_id: str) -> gcp.iam.WorkloadIdentityPool | None:
    """
    Check if WIF pool exists, create if it doesn't.

    Args:
        project_id: GCP project ID

    Returns:
        WorkloadIdentityPool resource or None if it already exists
    """

    return gcp.iam.WorkloadIdentityPool(
        f'{project_id}-github-pool',
        workload_identity_pool_id=WIF_POOL_NAME,
        project=project_id,
        display_name='GitHub Actions Pool',
        description='Workload Identity Pool for GitHub Actions OIDC',
        disabled=False,
        opts=pulumi.ResourceOptions(
            # Don't fail if it already exists
            protect=True,
        ),
    )


def check_or_create_wif_provider(
    project_id: str,
    pool: gcp.iam.WorkloadIdentityPool,
) -> gcp.iam.WorkloadIdentityPoolProvider:
    """
    Check if WIF provider exists, create if it doesn't.

    Args:
        project_id: GCP project ID
        pool: WorkloadIdentityPool resource

    Returns:
        WorkloadIdentityPoolProvider resource
    """
    return gcp.iam.WorkloadIdentityPoolProvider(
        f'{project_id}-github-provider',
        workload_identity_pool_id=pool.workload_identity_pool_id,
        workload_identity_pool_provider_id=WIF_PROVIDER_NAME,
        project=project_id,
        display_name='GitHub Actions Provider',
        description=f'OIDC provider for {GITHUB_ORG} GitHub repositories',
        disabled=False,
        attribute_mapping={
            'google.subject': 'assertion.sub',
            'attribute.actor': 'assertion.actor',
            'attribute.repository': 'assertion.repository',
        },
        attribute_condition=f"assertion.repository_owner == '{GITHUB_ORG}'",
        oidc=gcp.iam.WorkloadIdentityPoolProviderOidcArgs(
            issuer_uri='https://token.actions.githubusercontent.com',
        ),
        opts=pulumi.ResourceOptions(
            depends_on=[pool],
            protect=True,
        ),
    )


def create_github_service_account(
    project_id: str,
    repo_name: str,
    environment: str,
    github_repo: str,
) -> gcp.serviceaccount.Account:
    """
    Create a service account for a GitHub repository environment.

    Args:
        project_id: GCP project ID
        repo_name: Repository name
        environment: Environment name
        github_repo: Full GitHub repo path (org/repo)

    Returns:
        Service account resource
    """
    sa_name = sanitize_sa_name(repo_name, environment)

    return gcp.serviceaccount.Account(
        f'{project_id}-{sa_name}',
        account_id=sa_name,
        display_name=f'Deploy {github_repo} {environment} images',
        project=project_id,
        create_ignore_already_exists=True,
    )


def grant_wif_impersonation(
    resource_key: str,
    service_account: gcp.serviceaccount.Account,
    project_number: str,
    github_repo: str,
    environment: str,
) -> gcp.serviceaccount.IAMMember:
    """
    Grant WIF permission to impersonate the service account.

    Args:
        resource_key: Unique resource key for Pulumi
        service_account: Service account to grant access to
        project_number: GCP project number
        github_repo: Full GitHub repo path
        environment: Environment name

    Returns:
        IAM member binding
    """
    # Create the principal identifier for WIF
    # Format: principal://iam.googleapis.com/projects/{PROJECT_NUMBER}/locations/global/
    #         workloadIdentityPools/{POOL}/subject/repo:{ORG}/{REPO}:environment:{ENV}
    principal = pulumi.Output.concat(
        f'principal://iam.googleapis.com/projects/{project_number}/',
        f'locations/global/workloadIdentityPools/{WIF_POOL_NAME}/',
        f'subject/repo:{github_repo}:environment:{environment}',
    )

    return gcp.serviceaccount.IAMMember(
        resource_key,
        service_account_id=service_account.name,
        role='roles/iam.workloadIdentityUser',
        member=principal,
    )


def grant_artifact_registry_access(
    resource_key: str,
    service_account: gcp.serviceaccount.Account,
    project_id: str,
    location: str,
    registry_name: str,
    access: Literal['read', 'write'],
) -> gcp.artifactregistry.RepositoryIamMember:
    """
    Grant Artifact Registry access to a service account.

    Args:
        resource_key: Unique resource key for Pulumi
        service_account: Service account to grant access to
        project_id: GCP project ID
        location: Registry location
        registry_name: Name of the artifact registry
        access: Type of access ('read' or 'write')

    Returns:
        IAM member binding
    """
    role = (
        'roles/artifactregistry.writer'
        if access == 'write'
        else 'roles/artifactregistry.reader'
    )

    member = pulumi.Output.concat('serviceAccount:', service_account.email)

    return gcp.artifactregistry.RepositoryIamMember(
        resource_key,
        project=project_id,
        location=location,
        repository=registry_name,
        role=role,
        member=member,
    )


def setup_github_wif_infrastructure(
    config: dict[str, Any],
) -> dict[str, Any]:
    """
    Main function to set up GitHub WIF infrastructure from parsed config.

    Args:
        config: Parsed configuration dictionary (from external wrapper script)

    Returns:
        Dictionary of outputs for each repository/environment combination
    """
    validate_github_wif_config(config)

    for project_id, project_config in config['projects'].items():
        project_number = project_config['project_number']
        location = project_config['location']

        # Create or reference WIF pool and provider for this project
        pool = check_or_create_wif_pool(project_id)
        check_or_create_wif_provider(project_id, pool)

        # Get the provider resource name for outputs
        provider_name = get_wif_provider_resource_name(project_number)

        for repo_config in project_config.get('repositories', []):
            repo_name = repo_config['name']
            github_repo = repo_config['github_repo']

            for env_config in repo_config.get('environments', []):
                env_name = env_config['name']
                push_registry = env_config['push_registry']
                read_registries = env_config.get('read_registries', [])

                # Create service account
                sa = create_github_service_account(
                    project_id,
                    repo_name,
                    env_name,
                    github_repo,
                )

                # Grant WIF impersonation permission
                grant_wif_impersonation(
                    f'{project_id}-{repo_name}-{env_name}-wif-binding',
                    sa,
                    project_number,
                    github_repo,
                    env_name,
                )

                # Grant write access to push registry
                grant_artifact_registry_access(
                    f'{project_id}-{repo_name}-{env_name}-{push_registry}-write',
                    sa,
                    project_id,
                    location,
                    push_registry,
                    'write',
                )

                # Grant read access to any read registries
                for idx, read_registry in enumerate(read_registries):
                    grant_artifact_registry_access(
                        f'{project_id}-{repo_name}-{env_name}-{read_registry}-read-{idx}',
                        sa,
                        project_id,
                        location,
                        read_registry,
                        'read',
                    )

                # Manage GitHub Secrets
                manage_github_secrets(
                    github_repo,
                    env_name,
                    provider_name,
                    sa.email,
                )

    return {}


def manage_github_secrets(
    github_repo: str,
    environment: str,
    provider_name: pulumi.Output[str],
    service_account_email: pulumi.Output[str],
) -> None:
    """
    Create GitHub secrets for WIF in the specified repository environment.

    Args:
        github_repo: Full GitHub repo path (org/repo)
        environment: Environment name
        provider_name: WIF provider resource name
        service_account_email: Service account email
    """
    import pulumi_github as github

    # Parse org and repo
    if '/' not in github_repo:
        # Should be caught by validation, but safe fallback
        return

    org_name, repo_name = github_repo.split('/', 1)

    # Configure the GitHub provider explicitly for this organization
    # This ensures it looks for the repo in the correct org, not the user's profile
    gh_provider = github.Provider(
        f'{repo_name}-{environment}-provider',
        owner=org_name,
    )

    # Ensure the environment exists
    # We use a resource name that includes the repo to avoid collisions if multiple repos use same env name

    # Sanitize environment name for Pulumi resource name
    env_resource_name = f'{repo_name}-{environment}-env'

    # Configure deployment branch policy for production
    deployment_branch_policy = None
    if environment == 'production':
        deployment_branch_policy = (
            github.RepositoryEnvironmentDeploymentBranchPolicyArgs(
                protected_branches=True,
                custom_branch_policies=False,
            )
        )

    repo_env = github.RepositoryEnvironment(
        env_resource_name,
        repository=repo_name,
        environment=environment,
        deployment_branch_policy=deployment_branch_policy,
        opts=pulumi.ResourceOptions(provider=gh_provider),
    )

    # Create WIF_PROVIDER secret
    github.ActionsEnvironmentSecret(
        f'{env_resource_name}-wif-provider',
        repository=repo_name,
        environment=environment,
        secret_name='WIF_PROVIDER',  # noqa: S106
        plaintext_value=provider_name,
        opts=pulumi.ResourceOptions(depends_on=[repo_env], provider=gh_provider),
    )

    # Create WIF_SERVICE_ACCOUNT secret
    github.ActionsEnvironmentSecret(
        f'{env_resource_name}-wif-sa',
        repository=repo_name,
        environment=environment,
        secret_name='WIF_SERVICE_ACCOUNT',  # noqa: S106
        plaintext_value=service_account_email,
        opts=pulumi.ResourceOptions(depends_on=[repo_env], provider=gh_provider),
    )
