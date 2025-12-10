# GitHub Workload Identity Federation Setup

This module provides a standalone Pulumi stack for setting up GitHub Workload Identity Federation (WIF) with GCP Artifact Registry access.

## Overview

The GitHub WIF module allows you to:

- Define GitHub repositories and environments in a YAML configuration file
- Automatically create service accounts for each repository/environment combination
- Grant WIF impersonation permissions using GitHub OIDC tokens
- Assign Artifact Registry read/write permissions
- Export service account details for use in GitHub Actions

## Prerequisites

- Existing WIF pool: `github-pool`
- Existing WIF provider: `github-provider` (configured for `populationgenomics` organization)
- GCP projects with Artifact Registry repositories
- Pulumi installed and configured

## Usage

### 1. Create `github_wif.yaml`

Create a configuration file (e.g., `github_wif.yaml`) defining your repositories:

```yaml
projects:
  my-project-id:
    project_number: "123456789"
    location: "australia-southeast1"

    repositories:
      - name: my-repo
        github_repo: populationgenomics/my-repo

        environments:
          - name: development
            push_registry: images-dev
            read_registries:
              - images  # Read from prod registry for base images

          - name: production
            push_registry: images
            read_registries: []
```

### 2. Run the Stack

You can run this stack independently. **You must provide a `GITHUB_TOKEN`** with permissions to manage environments and secrets in your repositories.

```bash
# Navigate to the stack directory
cd cpg_infra/github_wif

# Set the config path (optional, defaults to github_wif.yaml in current dir)
export GITHUB_WIF_CONFIG=/path/to/your/github_wif.yaml

# Set GitHub Token
export GITHUB_TOKEN=ghp_...

# Initialize Pulumi stack (if first time)
pulumi stack init github_wif

# Preview changes
pulumi preview

# Deploy
pulumi up
```

**Automated Secrets**: This stack will automatically:

1. Create the GitHub Environment (if it doesn't exist).
2. Create `WIF_PROVIDER` and `WIF_SERVICE_ACCOUNT` secrets in that environment.

## Service Account Naming

Service accounts are automatically named:

- **Pattern**: `{repo-name}-img-{env}-deployer`
- **Examples**:
  - `my-repo-img-dev-deployer` (development environment)
  - `my-repo-img-prod-deployer` (production environment)
- **Environment mapping**: `development` → `dev`, `production` → `prod`
- **Long names**: Automatically truncated to fit GCP's 30-character limit

## Using in GitHub Actions

### Basic Workflow

```yaml
name: Build and Push Image

on:
  push:
    branches: [main]

permissions:
  contents: read
  id-token: write  # Required for WIF

jobs:
  build:
    runs-on: ubuntu-latest
    environment: development  # Must match YAML config

    steps:
      - uses: actions/checkout@v4

      - id: auth
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.WIF_PROVIDER }}
          service_account: ${{ secrets.WIF_SERVICE_ACCOUNT }}

      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v2

      - name: Configure Docker
        run: gcloud auth configure-docker australia-southeast1-docker.pkg.dev

      - name: Build and Push
        run: |
          IMAGE=australia-southeast1-docker.pkg.dev/my-project-id/images-dev/my-image:${{ github.sha }}
          docker build -t $IMAGE .
          docker push $IMAGE
```

The secrets `WIF_PROVIDER` and `WIF_SERVICE_ACCOUNT` are automatically created in your GitHub repository's environment by the Pulumi stack.

## Permissions

Each service account receives:

1. **WIF Impersonation** (`roles/iam.workloadIdentityUser`):
   - Scoped to specific GitHub repo and environment
   - Principal: `principalSet://iam.googleapis.com/projects/{PROJECT_NUMBER}/locations/global/workloadIdentityPools/github-pool/attribute.repository/populationgenomics/{repo}:environment:{env}`

2. **Artifact Registry Writer** (`roles/artifactregistry.writer`):
   - On the specified `push_registry`

3. **Artifact Registry Reader** (`roles/artifactregistry.reader`):
   - On each registry in `read_registries`

## Example: Dev/Prod Configuration

Typical setup where dev builds on prod base images:

```yaml
projects:
  cpg-my-project-dev:
    project_number: "111111111"
    location: "australia-southeast1"
    repositories:
      - name: my-app
        github_repo: populationgenomics/my-app
        environments:
          - name: development
            push_registry: images-dev
            read_registries: [images]  # Read prod images

  cpg-my-project:
    project_number: "222222222"
    location: "australia-southeast1"
    repositories:
      - name: my-app
        github_repo: populationgenomics/my-app
        environments:
          - name: production
            push_registry: images
            read_registries: []  # No external reads needed
```

## Troubleshooting

### Validation Errors

If `pulumi up` fails with validation errors:

- Check YAML syntax with `python -c "import yaml; yaml.safe_load(open('github_wif.yaml'))"`
- Ensure all required fields are present: `project_number`, `location`, `repositories`
- Verify `github_repo` follows format: `populationgenomics/{repo-name}`

### Permission Denied in GitHub Actions

Ensure:

- GitHub repo owner matches `populationgenomics`
- Workflow has `permissions: id-token: write`
- Environment name in workflow matches YAML exactly
- The GitHub Environment exists (it should be automatically created by Pulumi)

### GitHub API 403 Errors

If you see `403 Resource not accessible by personal access token` errors:

- Ensure your `GITHUB_TOKEN` is a **Classic** Personal Access Token (not Fine-grained)
- The token must have the `repo` scope enabled
- **Authorize SSO** for the `populationgenomics` organization:
  - Go to **Settings** → **Developer settings** → **Personal access tokens**
  - Click **Configure SSO** next to your token
  - Click **Authorize** for `populationgenomics`

### Service Account Name Too Long

If repository names are very long:

- Use a shorter `name` field (different from `github_repo`)
- Names are automatically truncated, but explicit short names are clearer

## Adding New Repositories

1. Add repository config to `github_wif.yaml`
2. Run `pulumi up`
3. The GitHub secrets will be automatically created in the repository environment
4. Create a GitHub Actions workflow using the secrets (see example above)
