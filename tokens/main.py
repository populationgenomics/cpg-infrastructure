# pylint: disable=import-error
"""Copies Hail tokens from Kubernetes to the Google Secret Manager."""

import base64
import json
from pathlib import Path

import kubernetes.client
import kubernetes.config
import yaml
from google.cloud import secretmanager

# List of repos that are allowed for *all* datasets.
ALWAYS_ALLOWED_REPOS = ['analysis-runner', 'sample-metadata', 'production-pipelines']
TOKEN_DIR = Path(__file__).parent
REPO_ROOT = TOKEN_DIR.parent

# dataset -> list of git repos
repo_map_path = TOKEN_DIR / 'repository-map.json'
with open(repo_map_path, encoding='utf-8') as allowed_repo_file:
    ALLOWED_REPOS = json.load(allowed_repo_file)
    print(f'Loaded repository-map with {len(ALLOWED_REPOS)} keys')

GCP_PROJECT = 'analysis-runner'

kubernetes.config.load_kube_config()
kube_client = kubernetes.client.CoreV1Api()

secret_manager = secretmanager.SecretManagerServiceClient()


def add_secret(name: str, value: str) -> None:
    """Adds the given secret to the Secret Manager as a new version."""
    payload = value.encode('UTF-8')
    secret_path = secret_manager.secret_path(GCP_PROJECT, name)
    response = secret_manager.add_secret_version(
        request={'parent': secret_path, 'payload': {'data': payload}}
    )
    print(response.name)


def get_token(hail_user: str) -> str:
    """Returns the Hail token for the given user."""
    kube_secret_name = f'{hail_user}-tokens'
    kube_secret = kube_client.read_namespaced_secret(kube_secret_name, 'default')
    secret_data = kube_secret.data['tokens.json']
    hail_token = json.loads(base64.b64decode(secret_data))['default']
    return hail_token


def get_project_id(dataset: str) -> str:
    """Returns the GCP project ID associated with the given dataset."""
    with open(REPO_ROOT / f'stack/Pulumi.{dataset}.yaml', encoding='utf-8') as f:
        return yaml.safe_load(f)['config']['gcp:project']


def get_hail_user(dataset: str, access_level: str) -> str | None:
    """
    Returns the hail user associated with the given dataset for the access level
    """
    with open(REPO_ROOT / f'stack/Pulumi.{dataset}.yaml', encoding='utf-8') as f:
        config = yaml.safe_load(f)['config']
        key = f'datasets:gcp_hail_service_account_{access_level}'
        val = config.get(key)
        if not val:
            return None
        # removes -\d{3}@hail-295901.iam.gserviceaccount.com
        service_account_name = config[key][:-40]
        return service_account_name


def main():
    """Main entry point."""

    config = {}
    for dataset, allowed_repos in ALLOWED_REPOS.items():
        entries = {
            'projectId': get_project_id(dataset),
            'allowedRepos': list(set(ALWAYS_ALLOWED_REPOS + allowed_repos)),
        }
        for access_level in 'test', 'standard', 'full':
            hail_user = get_hail_user(dataset, access_level)
            if not hail_user:
                print(f'Warning: no Hail user found for {dataset}/{access_level}')
                entries = None
                break
            entries[f'{access_level}Token'] = get_token(hail_user)
        if entries:
            config[dataset] = entries

    add_secret('server-config', json.dumps(config))


if __name__ == '__main__':
    main()
