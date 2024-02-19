"""
To help transition away from pulumi's gcp group membership construct to a custom one,
download the state file and remove the necessary bits

"""

import json

from google.cloud import storage


def update_json():
    storage_client = storage.Client()
    source_bucket = storage_client.bucket("cpg-pulumi-state")
    source_blob = source_bucket.blob(".pulumi/stacks/production.json")
    data_str = source_blob.download_as_text()

    with open('production-old.json', 'a') as f:
        f.write(data_str)

    data = json.loads(data_str)
    data['checkpoint']['latest']['resources'] = [
        r
        for r in data['checkpoint']['latest']['resources']
        if r['type'] != 'gcp:cloudidentity/groupMembership:GroupMembership'
    ]

    result = json.dumps(data, indent=4)

    with open('production.json', 'a') as f:
        f.write(result)


update_json()
