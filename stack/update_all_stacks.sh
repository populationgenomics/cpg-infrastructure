#!/bin/bash

set -ex

for f in Pulumi.*.yaml
do
    DATASET=$(echo $f | cut -d . -f 2)
    echo "=== $DATASET ==="
    PULUMI_CONFIG_PASSPHRASE= pulumi stack select $DATASET
    PULUMI_CONFIG_PASSPHRASE= pulumi up -y
done
