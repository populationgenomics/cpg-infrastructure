"""Utilities for analysis-runner Pulumi stack management."""

import subprocess


def get_pulumi_config_passphrase() -> str:
    """Returns the GCP secret containing the Pulumi config passphrase."""

    return subprocess.check_output(
        [
            'gcloud',
            '--project=analysis-runner',
            'secrets',
            'versions',
            'access',
            'latest',
            '--secret=pulumi-passphrase',
        ],
        encoding='UTF-8',
    )
