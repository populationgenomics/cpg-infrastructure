#!/usr/bin/env python3

"""After buckets have been manually migrated to use Autoclass,
this script modifies the Pulumi state to reflect those changes."""

import sys
import json


def main():
    """Main entry point."""

    if len(sys.argv) < 4:
        print(f'syntax: {sys.argv[0]} <input.json> <output.json> <bucket-names>...')
        sys.exit(1)

    with open(sys.argv[1], 'rt', encoding='utf-8') as f:
        content = json.load(f)

    bucket_names = set(sys.argv[3:])
    for resource in content['checkpoint']['latest']['resources']:
        if (
            resource['type'] == 'gcp:storage/bucket:Bucket'
            and resource['id'] in bucket_names
        ):
            resource['inputs']['autoclass'] = {
                '__defaults': [],
                'enabled': True,
            }
            resource['outputs']['autoclass'] = {
                'enabled': True,
            }
            resource['propertyDependencies']['autoclass'] = None

            bucket_names.remove(resource['id'])
            if not bucket_names:
                break  # All done.

    if bucket_names:
        print(f'Error: could not find the following buckets {bucket_names}')
        sys.exit(1)

    with open(sys.argv[2], 'wt', encoding='utf-8') as f:
        json.dump(content, f, indent=4)


if __name__ == '__main__':
    main()
