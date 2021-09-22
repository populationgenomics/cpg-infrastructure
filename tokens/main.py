"""Copies Hail tokens from Kubernetes to the Google Secret Manager."""

import base64
import json
import kubernetes.client
import kubernetes.config
import yaml
from google.cloud import secretmanager


ALLOWED_REPOS = {
    'acute-care': ['sample-metadata'],
    'ancestry': ['ancestry'],
    'fewgenomes': ['analysis-runner', 'fewgenomes', 'joint-calling', 'sv-workflows'],
    'perth-neuro': ['sample-metadata'],
    'seqr': ['hail-elasticsearch-pipelines', 'sample-metadata'],
    'thousand-genomes': [],
    'tob-wgs': ['ancestry', 'joint-calling', 'tob-wgs', 'sv-workflows'],
    'mgrb': ['sample-metadata'],
}

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
    with open(f'../stack/Pulumi.{dataset}.yaml', encoding='utf-8') as f:
        return yaml.safe_load(f)['config']['gcp:project']


def main():
    """Main entry point."""
    config = {}
    for dataset, allowed_repos in ALLOWED_REPOS.items():
        entries = {'projectId': get_project_id(dataset), 'allowedRepos': allowed_repos}
        for access_level in 'test', 'standard', 'full':
            hail_user = f'{dataset}-{access_level}'
            entries[f'{access_level}Token'] = get_token(hail_user)
        config[dataset] = entries

    add_secret('server-config', json.dumps(config))


if __name__ == '__main__':
    main()
