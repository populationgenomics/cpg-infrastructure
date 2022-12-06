import glob
import json
from collections import defaultdict

import yaml

super_config = {}

KEYS_TO_IGNORE = {
    'datasets:customer_id',
    'gcp:user_project_override',
    'gcp:billing_project',
}

for filename in glob.glob('Pulumi.*.yaml'):
    dataset = filename.split('.')[1]
    if dataset == 'production':
        continue

    with open(filename, encoding='utf-8') as f:
        parsed = yaml.safe_load(f)
        if not parsed:
            print(f'Couldn\'t parse {filename}')
            continue

    parsed_config = parsed['config']
    if not parsed_config:
        print(f'Couldn\'t combine {filename}')
        continue

    config = defaultdict(dict, {
        k[9:]: v
        for k, v in parsed_config.items()
        if k.startswith('datasets:') and k not in KEYS_TO_IGNORE
    })

    config['gcp'] = {
        k[4:]: v
        for k, v in parsed_config.items()
        if k.startswith('gcp:') and k not in KEYS_TO_IGNORE
    }

    gcp_hail_keys = {
        'gcp_hail_service_account_full': 'hail_service_account_full',
        'gcp_hail_service_account_standard': 'hail_service_account_standard',
        'gcp_hail_service_account_test': 'hail_service_account_test',
    }
    azure_hail_keys = {
        'azure_hail_service_account_full': 'hail_service_account_full',
        'azure_hail_service_account_standard': 'hail_service_account_standard',
        'azure_hail_service_account_test': 'hail_service_account_test',
    }

    for old_key, new_key in gcp_hail_keys.items():
        if old_key not in config:
            continue

        config['gcp'][new_key] = config.pop(old_key)

    for old_key, new_key in azure_hail_keys.items():
        if old_key not in config:
            continue

        config['azure'][new_key] = config.pop(old_key)

    # other special handling
    if enable_release := config.get('enable_release'):
        if not isinstance(enable_release, bool):
            config['enable_release'] = json.loads(enable_release)
    if archive_age := config.get('archive_age'):
        if not isinstance(archive_age, int):
            config['archive_age'] = int(archive_age)

    if depends_on := config.get('depends_on'):
        config['depends_on'] = list(sorted(json.loads(depends_on)))

    super_config[dataset] = dict(config)

with open('production.yaml', 'w+', encoding='utf-8') as f:
    f.write(yaml.dump(super_config))
