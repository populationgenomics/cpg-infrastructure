# Autoclass migration

This folder contains scripts to help with the automation of migrating Google Cloud Storage buckets to be [Autoclass](https://cloud.google.com/storage/docs/autoclass)-enabled. At the time of writing, the Autoclass setting can only be applied at bucket _creation_ time, hence shuffling all data to an intermediate bucket (and back) is necessary, incurring some downtime.

We use [Cloud Batch](https://cloud.google.com/batch/docs/create-run-job) to execute the migration, as [Cloud Run jobs](https://cloud.google.com/run/docs/quickstarts/jobs/create-execute) are limited to 1h of execution time and 4 cores.

We use VMs with a moderate number of cores, as that helps with parallelizing the copy of many small blobs.

The dedicated service account `autoclass-migration@cpg-common.iam.gserviceaccount.com` must exist and have permissions to create and delete buckets, as well as perform the data transfer. Also make sure that the necessary [Cloud Batch permissions](https://cloud.google.com/batch/docs/get-started#project-prerequisites) have been granted.

## How to run

Switch Google Cloud projects:

```sh
gcloud config set project cpg-common
```

Then for each bucket `$BUCKET` to migrate, set the environment variable (e.g. `export BUCKET=cpg-fewgenomes-test` _without_ `gs://` prefix) and run:

```sh
export SLACK_WEBHOOK=$(gcloud secrets versions access latest --secret=slack-autoclass-migration-webhook)

# Assumes $BUCKET is exported.
gcloud batch jobs submit autoclass-migrate-$BUCKET \
    --config=cloud_batch_config.json \
    --location=asia-southeast1 \
    --machine-type=e2-highcpu-16 \
    --script-text="$(envsubst < migrate_bucket.sh)"
```

We're using `asia-southeast1` instead of `australia-southeast1` above, as at the time of writing, Cloud Batch is not available in the latter region. Since we're not copying data from bucket to bucket (without routing it through the VM), that doesn't cause additional network egress fees.
