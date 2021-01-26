Copies the authentication tokens for Hail Batch service accounts from
Kubernetes secrets to the Secret Manager. There it is accessible by the server
that launches Batch pipelines on behalf of users.

In order to run this, you must have access to the Kubernetes cluster:

```batch
gcloud container clusters get-credentials vdc
```

Install the Python dependencies using a conda environment:

```batch
conda create --name analysis-tokens python=3.9.0
conda activate analysis-tokens
pip install kubernetes==12.0.1 google-cloud-secret-manager==2.2.0 
```

The list of projects is hardcoded in `main.py`. To update the secret stored in
Secret Manager:

```batch
gcloud config set project analysis-runner
python3 main.py
```
