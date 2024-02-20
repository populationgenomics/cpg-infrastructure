"""
To help transition away from pulumi's gcp group membership construct to a custom one,
download the state file and remove the necessary bits

"""

import json
import os

from google.cloud import storage


def update_json():
    if os.path.exists('production-old.json'):
        with open('production-old.json', encoding='utf-8') as f:
            data_str = f.read()
    else:
        storage_client = storage.Client()
        source_bucket = storage_client.bucket("cpg-pulumi-state")
        source_blob = source_bucket.blob(".pulumi/stacks/production.json")
        data_str = source_blob.download_as_text()

        with open('production-old.json', 'w+', encoding='utf-8') as f:
            f.write(data_str)

    data = json.loads(data_str)
    len_before = len(data['checkpoint']['latest']['resources'])
    data['checkpoint']['latest']['resources'] = [
        r
        for r in data['checkpoint']['latest']['resources']
        if r['type'] != 'gcp:cloudidentity/groupMembership:GroupMembership'
    ]

    len_after = len(data['checkpoint']['latest']['resources'])
    print(f"Removed {len_before - len_after} group memberships")

    result = json.dumps(data, indent=4)

    with open('production.json', 'w+', encoding='utf-8') as f:
        f.write(result)


update_json()
