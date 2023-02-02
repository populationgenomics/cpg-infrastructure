#!/bin/bash

set -o pipefail
set -ex

if [[ -z "$BUCKET" ]]; then
    echo "No bucket specified"
    exit 1
fi

if [[ -z "$SLACK_WEBHOOK" ]]; then
    echo "No Slack webhook"
    exit 1
fi

BUCKET=gs://$BUCKET  # Add gs:// prefix

# First check that Autoclass hasn't already been enabled.
gsutil autoclass get $BUCKET | grep False

curl -X POST -H 'Content-type: application/json' \
    --data '{"text":"Test notification"}' \
    $SLACK_WEBHOOK

echo "All done"
