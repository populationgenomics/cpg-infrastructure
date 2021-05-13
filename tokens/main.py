"""Copies Hail tokens from Kubernetes to the Google Secret Manager."""

import base64
import json
import kubernetes.client
import kubernetes.config
from google.cloud import secretmanager


ALLOWED_REPOS = {
    'ancestry': ['ancestry'],
    'tob-wgs': ['ancestry', 'joint-calling', 'tob-wgs'],
    'fewgenomes': ['analysis-runner', 'fewgenomes', 'joint-calling'],
    'seqr': ['hail-elasticsearch-pipelines'],
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


def get_token(hail_user: str):
    """Returns the Hail token for the given user."""
    kube_secret_name = f'{hail_user}-tokens'
    kube_secret = kube_client.read_namespaced_secret(kube_secret_name, 'default')
    secret_data = kube_secret.data['tokens.json']
    hail_token = json.loads(base64.b64decode(secret_data))['default']
    return hail_token


config = {}
for dataset, allowed_repos in ALLOWED_REPOS.items():
    config[dataset] = {'allowedRepos': allowed_repos}
    for access_level in 'test', 'standard', 'full':
        config[dataset][f'{access_level}Token'] = get_token(f'{dataset}-{access_level}')

add_secret('server-config', json.dumps(config))
