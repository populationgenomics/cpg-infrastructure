# Autoclass migration

This folder contains a [script](migrate_bucket.sh) to automate the migration of Google Cloud Storage buckets to [Autoclass](https://cloud.google.com/storage/docs/autoclass). At the time of writing, the Autoclass setting can only be applied at bucket _creation_ time, hence shuffling all data to an intermediate bucket (and back) is necessary, incurring some downtime.

We use [Cloud Batch](https://cloud.google.com/batch/docs/create-run-job) to execute the migration, as [Cloud Run jobs](https://cloud.google.com/run/docs/quickstarts/jobs/create-execute) are limited to 1h of execution time and 4 cores.

We use VMs with a moderate number of cores, as that helps with parallelizing the copy of many small blobs.

The dedicated service account `autoclass-migration@cpg-common.iam.gserviceaccount.com` must exist and have permissions to create and delete buckets, as well as perform the data transfer. Also make sure that the necessary [Cloud Batch](https://cloud.google.com/batch/docs/get-started#project-prerequisites) and Artifact Registry Reader permissions have been granted.

To help keep track of the migration progress, the script sends messages to a Slack channel. The corresponding Slack webhook needs to be stored in the `slack-autoclass-migration-webhook` secret.

## How to run

Switch Google Cloud projects:

```sh
gcloud config set project cpg-common
```

If not already present, build the Docker image:

```sh
gcloud builds submit --tag australia-southeast1-docker.pkg.dev/cpg-common/images/autoclass-migration:latest .
```

Start a batch job for each bucket to migrate, e.g. to migrate all buckets in the `fewgenomes` project, using the billing project `cpg-common`:

```sh
GCP_PROJECT=fewgenomes
export BILLING_PROJECT=cpg-common
export SLACK_WEBHOOK=$(gcloud secrets versions access latest --secret=slack-autoclass-migration-webhook)

for b in $(gcloud storage ls --project=$GCP_PROJECT); do
    export BUCKET=$(echo $b | cut -f 3 -d '/')
    # Only consider buckets that have a "cpg-" prefix.
    # Don't change the "-archive" bucket, as that's immediate cold storage.
    if [[ $BUCKET = cpg-* && $BUCKET != *-archive ]]; then
        gcloud batch jobs submit autoclass-migrate-$BUCKET \
            --config=<(envsubst < cloud_batch_config_template.json) \
            --location=asia-southeast1
    fi
done
```

We're using `asia-southeast1` instead of `australia-southeast1` above, as at the time of writing, Cloud Batch is not available in Australia. Since we're not copying data from bucket to bucket (without routing it through the VM), that doesn't cause additional network egress fees.
