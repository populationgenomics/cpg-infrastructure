#!/bin/bash

set -o pipefail
set -ex

if [[ -z "$GCP_PROJECT" ]]; then
    echo "No GCP project specified"
    exit 1
fi

if [[ -z "$BUCKET" ]]; then
    echo "No bucket specified"
    exit 1
fi

if [[ -z "$BILLING_PROJECT" ]]; then
    echo "No billing project specified"
    exit 1
fi

if [[ -z "$SLACK_WEBHOOK" ]]; then
    echo "No Slack webhook specified"
    exit 1
fi

post_to_slack() {
    curl --fail --silent --show-error  -X POST -H 'Content-type: application/json' -d '{"text":"'"$1"'"}' "$SLACK_WEBHOOK"
}

report_exit_status() {
    rv=$?

    if [[ $rv = 0 ]]; then
        post_to_slack ":white_check_mark: migration for $BUCKET finished successfully"
    else
        post_to_slack ":x: migration for $BUCKET failed"
    fi

    exit $rv
}

trap "report_exit_status" EXIT

post_to_slack "Starting migration for $BUCKET"

# First check that Autoclass hasn't already been enabled.
if gsutil -u "$BILLING_PROJECT" autoclass get "gs://$BUCKET" | grep True; then
    post_to_slack "Autoclass is already enabled for $BUCKET"
    exit 0
fi

# Store the IAM permissions.
gsutil -u "$BILLING_PROJECT" iam get "gs://$BUCKET" > /tmp/iam.json

# Store the object versioning setting.
OBJECT_VERSIONING=$(gsutil -u "$BILLING_PROJECT" versioning get "gs://$BUCKET" | cut -f 2 -d ' ')

# Store the lifecycle configuration.
gsutil -u "$BILLING_PROJECT" lifecycle get "gs://$BUCKET" > /tmp/lifecycle_config.json

# Store the Requester Pays setting.
REQUESTER_PAYS=$(gsutil -u "$BILLING_PROJECT" requesterpays get "gs://$BUCKET" | cut -f 2 -d ' ')

# Remove all IAM permissions to prevent modifications while we perform the temporary copy.
gsutil -u "$BILLING_PROJECT" iam set -e '' <(echo "{}") "gs://$BUCKET"

# Determine total size.
BUCKET_SIZE=$(gsutil -u "$BILLING_PROJECT" du -s "gs://$BUCKET" | cut -f 1 -d ' ')
BUCKET_SIZE_IEC=$(echo "$BUCKET_SIZE" | numfmt --to=iec)
post_to_slack "Bucket size for $BUCKET: ${BUCKET_SIZE_IEC}B"

# Only need to perform a copy if the bucket is non-empty.
if [[ $BUCKET_SIZE -gt 0 ]]; then
    # Create a temporary bucket.
    TMP_BUCKET=$BUCKET-autoclass-migration-tmp
    gsutil -u "$BILLING_PROJECT" mb -p "$GCP_PROJECT" -l australia-southeast1 -b on "gs://$TMP_BUCKET"

    # Copy all data to the temporary bucket.
    gsutil -u "$BILLING_PROJECT" -m -o GSUtil:parallel_thread_count=32 cp -r "gs://$BUCKET/*" "gs://$TMP_BUCKET"

    # Compare total bucket sizes to make sure the copy completed successfully.
    TMP_BUCKET_SIZE=$(gsutil -u "$BILLING_PROJECT" du -s "gs://$TMP_BUCKET" | cut -f 1 -d ' ')
    if [[ $BUCKET_SIZE -ne $TMP_BUCKET_SIZE ]]; then
        post_to_slack "Temporary copy size mismatch for $BUCKET: $BUCKET_SIZE B vs $TMP_BUCKET_SIZE B"
        exit 1
    fi
fi

# Delete the original bucket.
gsutil -u "$BILLING_PROJECT" -m -o GSUtil:parallel_thread_count=32 rm -r "gs://$BUCKET"

# Recreate the bucket, this time with Autoclass enabled.
gsutil -u "$BILLING_PROJECT" mb -p "$GCP_PROJECT" -l australia-southeast1 -b on --autoclass "gs://$BUCKET"

# Only need to perform a copy if the bucket is non-empty.
if [[ $BUCKET_SIZE -gt 0 ]]; then
    # Copy all data to back from the temporary bucket.
    gsutil -u "$BILLING_PROJECT" -m -o GSUtil:parallel_thread_count=32 cp -r "gs://$TMP_BUCKET/*" "gs://$BUCKET"

    # Compare total bucket sizes to make sure the copy completed successfully.
    BUCKET_SIZE=$(gsutil -u "$BILLING_PROJECT" du -s "gs://$BUCKET" | cut -f 1 -d ' ')
    if [[ $BUCKET_SIZE -ne $TMP_BUCKET_SIZE ]]; then
        post_to_slack "Back-copy size mismatch for $BUCKET: $BUCKET_SIZE B vs $TMP_BUCKET_SIZE B"
        exit 1
    fi

    # Delete the temporary bucket.
    gsutil -u "$BILLING_PROJECT" -m -o GSUtil:parallel_thread_count=32 rm -r "gs://$TMP_BUCKET"
fi

# Restore object versioning.
if [[ $OBJECT_VERSIONING = "Enabled" ]]; then
    gsutil -u "$BILLING_PROJECT" versioning set on "gs://$BUCKET"
fi

# Restore the lifecycle configuration.
if ! grep "has no lifecycle configuration" /tmp/lifecycle_config.json; then
    gsutil -u "$BILLING_PROJECT" lifecycle set /tmp/lifecycle_config.json "gs://$BUCKET"
fi

# Restore the Requester Pays setting.
if [[ $REQUESTER_PAYS = "Enabled" ]]; then
    gsutil -u "$BILLING_PROJECT" requesterpays set on "gs://$BUCKET"
fi

# Restore the IAM permissions.
gsutil -u "$BILLING_PROJECT" iam set -e '' /tmp/iam.json "gs://$BUCKET"
