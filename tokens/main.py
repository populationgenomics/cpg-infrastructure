# pylint: disable=import-error
"""Copies Hail tokens from Kubernetes to the Google Secret Manager."""

from typing import Any
import base64
import json
from pathlib import Path
from collections import defaultdict

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


def get_token(environment: str, hail_user: str) -> str:
    """Returns the Hail token for the given user."""

    if not environment == 'gcp':
        raise NotImplementedError(
            f'Cannot get token for hail_user {hail_user} on {environment}'
        )

    kube_secret_name = f'{hail_user}-tokens'
    kube_secret = kube_client.read_namespaced_secret(kube_secret_name, 'default')
    secret_data = kube_secret.data['tokens.json']
    hail_token = json.loads(base64.b64decode(secret_data))['default']
    return hail_token


def main():
    """Main entry point."""

    with open('../stack/production.yaml') as f:
        production_config = yaml.safe_load(f)

    invalid_datasets = set(ALLOWED_REPOS.keys()) - set(production_config.keys())
    if invalid_datasets:
        raise ValueError(
            'Some datasets in allowed_repos were not contained in production.yaml: '
            + ', '.join(invalid_datasets)
        )

    config = {}

    for dataset, dataset_config in production_config.items():
        allowed_repos = ALLOWED_REPOS.get(dataset, [])
        gcp_project_id = dataset_config.get('gcp', {}).get('project')
        if not gcp_project_id:
            raise ValueError(
                f'Could not find GCP project ID for {dataset} in production.yaml'
            )

        entries: dict[str, dict[str, Any] | list[str]] = defaultdict(dict)

        for infra in ('gcp',):
            entries[infra] = {}
            infra_config = dataset_config.get(infra)
            if not infra_config:
                continue

            project_key = 'project' if infra == 'gcp' else 'subscriptionId'
            entries[infra]['projectId'] = infra_config.get(project_key)
            for access_level in 'test', 'standard', 'full':

                # removes -\d{3}@hail-295901.iam.gserviceaccount.com
                hail_user = infra_config.get(
                    f'hail_service_account_{access_level}', ''
                )[:-40]
                if not hail_user:
                    print(
                        f'Warning: no Hail user found for {infra}/{dataset}/{access_level}'
                    )
                    break
                entries[infra][f'{access_level}Token'] = get_token(
                    environment=infra, hail_user=hail_user
                )
        entries['allowedRepos'] = list(set(ALWAYS_ALLOWED_REPOS + allowed_repos))
        config[dataset] = entries

    add_secret('server-config', json.dumps(config))


if __name__ == '__main__':
    main()
