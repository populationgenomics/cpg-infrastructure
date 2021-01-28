# Copying Hail tokens

This utility copies the authentication tokens for Hail Batch service accounts
from Kubernetes secrets to the Secret Manager. There it is accessible by the
server that launches Batch pipelines on behalf of users.

In order to run this, you must have access to the Kubernetes cluster:

```batch
gcloud config set project hail-295901
gcloud container clusters get-credentials vdc
```

See the [main readme file](../README.md) about how to set up a conda
environment. The list of projects is hardcoded in `main.py`. To update the
secret stored in Secret Manager:

```batch
gcloud config set project analysis-runner
python3 main.py
```
