"""
Strip Azure resources from the production Pulumi checkpoint so cpg-infra can
retire Azure support without a subsequent `pulumi up` provoking destroy diffs
against resources it can no longer authenticate to.

SAFETY
------
This script edits a LOCAL JSON state file only. It makes no calls to any
Azure API, requires no Azure credentials, and never invokes
`pulumi up / refresh / destroy`. When the trimmed state is uploaded back,
Pulumi simply stops tracking the Azure resources -- the resources themselves
remain in Azure until removed out-of-band. This matches the semantics of
`pulumi state delete <urn>`; we do it in bulk via direct JSON editing because
there are too many URNs to remove one-by-one.

Default is dry-run; pass --apply to write the trimmed output.

Usage
-----
    # 1. Dry-run against a fresh download (reports what would be removed):
    python scripts/remove_azure_resources.py

    # 2. Write the trimmed state:
    #    production.json         -- full checkpoint, for gsutil-cp fallback
    #    production-import.json  -- deployment envelope, for pulumi stack import
    python scripts/remove_azure_resources.py --apply

    # 3. RECOMMENDED upload path -- pulumi validates and re-hashes the state:
    pulumi stack select datasets/production
    pulumi stack import --file production-import.json

    # 3b. FALLBACK upload path -- direct GCS write. This bypasses
    #     `pulumi stack import`, so Snapshot.VerifyIntegrity() runs only on
    #     the next `pulumi up/refresh`. If manifest.magic or the checkpoint
    #     schema ever drifts, this path can surface as an integrity error
    #     that only clears with `--disable-integrity-checking`. Prefer 3.
    gsutil cp production.json \
        gs://cpg-pulumi-state/.pulumi/stacks/datasets/production.json

    # 4. Verify from the consuming Pulumi program:
    pulumi refresh --preview   # should not surface any Azure URNs

The GCS blob path defaults to `.pulumi/stacks/datasets/production.json`; pass
`--gcs-blob <path>` if your stack lives elsewhere in the bucket.
"""

import argparse
import json
import os
import sys

AZURE_TYPE_PREFIXES = (
    'azure-native:',
    'azuread:',
    'pulumi:providers:azure-native',
    'pulumi:providers:azuread',
)

GCS_BUCKET = 'cpg-pulumi-state'
GCS_BLOB = '.pulumi/stacks/datasets/production.json'
BACKUP_PATH = 'production-old.json'
OUTPUT_PATH = 'production.json'
IMPORT_PATH = 'production-import.json'


def is_azure_resource(resource: dict) -> bool:
    resource_type = resource.get('type', '')
    if resource_type.startswith(AZURE_TYPE_PREFIXES):
        return True
    # cpg_infra builds resource names as `{dataset}-{cloud}-{key}` where
    # AzureInfra.name() returns 'azure'. Component resources without an
    # azure-native/azuread type can still be identified by the terminal
    # name segment of the URN containing 'azure' as a hyphen-delimited word
    # (e.g. `dataset-azure-key`). The URN separator is '::' so we peel the
    # name off with rsplit and match on token boundaries -- '::azure::' as a
    # substring never appears under this naming convention.
    urn = resource.get('urn', '')
    if not urn:
        return False
    name = urn.rsplit('::', 1)[-1]
    return 'azure' in name.split('-')


def _extract_provider_urn(provider_ref: str) -> str:
    """Pulumi provider references stored on a resource are `<urn>::<id>` --
    the URN itself contains `::` separators, so peel off the trailing `::<id>`
    with rsplit to recover just the URN portion.
    """
    if '::' in provider_ref:
        return provider_ref.rsplit('::', 1)[0]
    return provider_ref


def find_cross_cloud_refs(
    azure_urns: set[str], non_azure: list[dict]
) -> list[tuple[str, str, str]]:
    """Return (dependent_urn, ref_kind, azure_urn) for every non-Azure
    resource that references an Azure URN. These references become dangling
    once the Azure resources are removed; strip_azure_refs() cleans them up
    at --apply time.

    Covers every field Pulumi uses to point at another resource's URN:
    parent, dependencies, propertyDependencies, provider, providers,
    deletedWith, aliases. Missing any of these lets a dangling URN survive
    into the trimmed state and fails Snapshot.VerifyIntegrity() on the next
    `pulumi refresh` / `pulumi up`.
    """
    problems: list[tuple[str, str, str]] = []
    for res in non_azure:
        urn = res.get('urn', '<unknown>')

        parent = res.get('parent')
        if parent and parent in azure_urns:
            problems.append((urn, 'parent', parent))

        for dep in res.get('dependencies') or []:
            if dep in azure_urns:
                problems.append((urn, 'dependency', dep))

        for prop, deps in (res.get('propertyDependencies') or {}).items():
            for dep in deps or []:
                if dep in azure_urns:
                    problems.append((urn, f'propertyDependency[{prop}]', dep))

        provider = res.get('provider')
        if provider and _extract_provider_urn(provider) in azure_urns:
            problems.append((urn, 'provider', provider))

        for pkg, prov_ref in (res.get('providers') or {}).items():
            if prov_ref and _extract_provider_urn(prov_ref) in azure_urns:
                problems.append((urn, f'providers[{pkg}]', prov_ref))

        deleted_with = res.get('deletedWith')
        if deleted_with and deleted_with in azure_urns:
            problems.append((urn, 'deletedWith', deleted_with))

        for alias in res.get('aliases') or []:
            if isinstance(alias, str) and alias in azure_urns:
                problems.append((urn, 'alias', alias))

    return problems


def strip_azure_refs(resource: dict, azure_urns: set[str]) -> int:
    """Remove Azure URNs from this resource's DAG-bookkeeping fields
    in-place. Returns the count of URNs removed.

    Covers parent, dependencies, propertyDependencies, provider, providers,
    deletedWith, and aliases. Does not touch `type`, `inputs`, `outputs`, or
    any other field describing the resource itself -- only fields that point
    at the URNs of other resources. Kept in lockstep with
    find_cross_cloud_refs so the dry-run report matches what --apply does.
    """
    removed = 0

    parent = resource.get('parent')
    if parent and parent in azure_urns:
        del resource['parent']
        removed += 1

    deps = resource.get('dependencies')
    if deps:
        kept = [d for d in deps if d not in azure_urns]
        removed += len(deps) - len(kept)
        if kept:
            resource['dependencies'] = kept
        else:
            del resource['dependencies']

    prop_deps = resource.get('propertyDependencies')
    if prop_deps:
        for prop, urns in list(prop_deps.items()):
            current = urns or []
            kept = [u for u in current if u not in azure_urns]
            removed += len(current) - len(kept)
            if kept:
                prop_deps[prop] = kept
            else:
                del prop_deps[prop]
        if not prop_deps:
            del resource['propertyDependencies']

    provider = resource.get('provider')
    if provider and _extract_provider_urn(provider) in azure_urns:
        del resource['provider']
        removed += 1

    providers = resource.get('providers')
    if providers:
        for pkg, prov_ref in list(providers.items()):
            if prov_ref and _extract_provider_urn(prov_ref) in azure_urns:
                del providers[pkg]
                removed += 1
        if not providers:
            del resource['providers']

    deleted_with = resource.get('deletedWith')
    if deleted_with and deleted_with in azure_urns:
        del resource['deletedWith']
        removed += 1

    aliases = resource.get('aliases')
    if aliases:
        kept_aliases = [
            a for a in aliases if not (isinstance(a, str) and a in azure_urns)
        ]
        removed += len(aliases) - len(kept_aliases)
        if kept_aliases:
            resource['aliases'] = kept_aliases
        else:
            del resource['aliases']

    return removed


def load_state(gcs_blob: str) -> dict:
    """Load state from local backup if present; otherwise download from GCS.

    Downloaded content is written to BACKUP_PATH so the raw checkpoint is
    always recoverable regardless of what --apply does to OUTPUT_PATH.
    """
    if os.path.exists(BACKUP_PATH):
        print(f'Reading existing backup {BACKUP_PATH}')
        with open(BACKUP_PATH, encoding='utf-8') as f:
            return json.loads(f.read())

    print(f'Downloading gs://{GCS_BUCKET}/{gcs_blob} -> {BACKUP_PATH}')
    from google.cloud import storage  # - only needed on download

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_blob)
    data_str = blob.download_as_text()

    with open(BACKUP_PATH, 'w', encoding='utf-8') as f:
        f.write(data_str)

    return json.loads(data_str)


def summarise(azure: list[dict]) -> None:
    by_type: dict[str, int] = {}
    for r in azure:
        t = r.get('type', '<unknown>')
        by_type[t] = by_type.get(t, 0) + 1
    print('\nBreakdown by type:')
    for t, n in sorted(by_type.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f'  {n:>5}  {t}')


def describe_affected(res: dict) -> dict[str, str]:
    """Extract human-useful identifying info from a Pulumi state resource.
    Merges outputs then inputs (inputs win) so we see the actual configured
    values."""
    inputs = res.get('inputs') or {}
    outputs = res.get('outputs') or {}
    combined = {**outputs, **inputs}

    type_name = res.get('type', '<unknown>')
    if 'bucket' in combined:
        bucket = combined.get('bucket')
    elif type_name.endswith(':Bucket'):
        bucket = combined.get('name')
    else:
        bucket = None

    obj_name = combined.get('name')
    project = combined.get('project')
    location = combined.get('location') or combined.get('region')
    self_link = (
        combined.get('selfLink') or combined.get('url') or combined.get('mediaLink')
    )

    if bucket and obj_name and bucket != obj_name:
        gs_url = f'gs://{bucket}/{obj_name}'
    elif bucket:
        gs_url = f'gs://{bucket}'
    else:
        gs_url = '-'

    return {
        'type': type_name,
        'bucket': bucket or '-',
        'object': obj_name or '-',
        'gs_url': gs_url,
        'project': project or '-',
        'location': location or '-',
        'self_link': self_link or '-',
    }


def report_affected(
    problems: list[tuple[str, str, str]], non_azure: list[dict]
) -> None:
    """Group affected surviving resources by (type, bucket) and print a
    detailed listing so the operator can see exactly which GCS objects still
    carry a dangling Azure URN reference. Bucket / object name / URL / project
    are extracted from the resource's inputs or outputs."""
    by_urn: dict[str, dict] = {r.get('urn', ''): r for r in non_azure}
    affected_urns = sorted({urn for urn, _, _ in problems})

    by_bucket: dict[str, int] = {}
    by_type: dict[str, int] = {}
    details: list[dict[str, str]] = []
    for urn in affected_urns:
        res = by_urn.get(urn)
        if not res:
            continue
        info = describe_affected(res)
        info['urn'] = urn
        details.append(info)
        by_bucket[info['bucket']] = by_bucket.get(info['bucket'], 0) + 1
        by_type[info['type']] = by_type.get(info['type'], 0) + 1

    print('\nAffected surviving resources grouped by GCS bucket:')
    for b, n in sorted(by_bucket.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f'  {n:>4}  {b}')

    print('\nAffected surviving resources grouped by type:')
    for t, n in sorted(by_type.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f'  {n:>4}  {t}')

    print('\nAffected surviving resources (full detail):')
    for info in details:
        print(f'  URN: {info["urn"]}')
        print(f'    type:      {info["type"]}')
        print(f'    object:    {info["object"]}')
        print(f'    gs_url:    {info["gs_url"]}')
        print(f'    project:   {info["project"]}')
        print(f'    location:  {info["location"]}')
        if info['self_link'] != '-':
            print(f'    selfLink:  {info["self_link"]}')


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Write the trimmed state to production.json. Without this flag, '
        'the script only reports what would change.',
    )
    parser.add_argument(
        '--gcs-blob',
        default=GCS_BLOB,
        help=f'Blob path within gs://{GCS_BUCKET}/ to download when '
        f'{BACKUP_PATH} is not already present locally. '
        f'Default: {GCS_BLOB}',
    )
    args = parser.parse_args()

    data = load_state(args.gcs_blob)
    latest = data['checkpoint']['latest']
    resources = latest['resources']

    azure = [r for r in resources if is_azure_resource(r)]
    non_azure = [r for r in resources if not is_azure_resource(r)]
    azure_urns = {r.get('urn', '') for r in azure}

    # `pending_operations` records in-flight create/update/delete ops from an
    # interrupted `pulumi up`. An entry referencing an Azure URN that is no
    # longer in `resources` blocks the next `pulumi up` with 'the following
    # resources have pending operations... run pulumi cancel', so filter it
    # alongside the resources list.
    pending = latest.get('pending_operations') or []
    azure_pending = [
        op for op in pending if (op.get('resource') or {}).get('urn') in azure_urns
    ]
    kept_pending = [op for op in pending if op not in azure_pending]

    print(f'\nLoaded {len(resources)} resources.')
    print(f'  Azure resources to remove: {len(azure)}')
    print(f'  Non-Azure resources kept:  {len(non_azure)}')
    if pending:
        print(f'  Pending operations total:  {len(pending)}')
        print(f'    Azure pending to remove: {len(azure_pending)}')
        print(f'    Non-Azure pending kept:  {len(kept_pending)}')

    if not azure:
        print('\nNo Azure resources found. Nothing to do.')
        return 0

    summarise(azure)

    if azure_pending:
        print('\nAzure pending operations to drop:')
        for op in azure_pending:
            op_type = op.get('type', '<unknown>')
            urn = (op.get('resource') or {}).get('urn', '<unknown>')
            print(f'  {op_type:>8}  {urn}')

    problems = find_cross_cloud_refs(azure_urns, non_azure)
    if problems:
        distinct_dependents = {urn for urn, _, _ in problems}
        distinct_targets = {target for _, _, target in problems}
        print(
            f'\n{len(problems)} dangling reference edge(s) found: '
            f'{len(distinct_dependents)} surviving resource(s) reference '
            f'{len(distinct_targets)} Azure URN(s) via '
            'parent / dependencies / propertyDependencies / provider / '
            'providers / deletedWith / aliases. These are Pulumi DAG '
            'bookkeeping fields; --apply strips the Azure URNs from them '
            'in-place, leaving the depending resource itself untouched. '
            'Full edge list:'
        )
        for dep_urn, kind, azure_urn in problems:
            print(f'  {dep_urn}\n    {kind} -> {azure_urn}')

        report_affected(problems, non_azure)

    if not args.apply:
        print(
            f'\nDry-run only. Re-run with --apply to write the trimmed state '
            f'to {OUTPUT_PATH}.'
        )
        return 0

    refs_stripped = sum(strip_azure_refs(r, azure_urns) for r in non_azure)
    latest['resources'] = non_azure
    if 'pending_operations' in latest:
        if kept_pending:
            latest['pending_operations'] = kept_pending
        else:
            del latest['pending_operations']
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(json.dumps(data, indent=4))

    # `pulumi stack import` expects the deployment envelope, not the GCS
    # checkpoint wrapper. Emit that shape too so the operator can use the
    # safer upload path -- pulumi validates references and re-hashes
    # manifest.magic during import, which a direct gsutil cp does not.
    import_doc = {
        'version': data.get('version', 3),
        'deployment': latest,
    }
    with open(IMPORT_PATH, 'w', encoding='utf-8') as f:
        f.write(json.dumps(import_doc, indent=4))

    print(f'\nWrote {len(non_azure)} resources to {OUTPUT_PATH}.')
    print(f'Wrote pulumi-import envelope to {IMPORT_PATH}.')
    print(
        f'Stripped {refs_stripped} dangling Azure URN(s) from surviving ' 'resources.'
    )
    if azure_pending:
        print(
            f'Dropped {len(azure_pending)} Azure pending operation(s); '
            f'{len(kept_pending)} pending operation(s) kept.'
        )
    print(
        '\nThis script did NOT touch Azure. The Azure resources themselves '
        'still exist in Azure; they are simply no longer tracked in the '
        'checkpoint. To make the change take effect, upload the trimmed '
        'state via ONE of:'
    )
    print(
        f'  RECOMMENDED: pulumi stack select datasets/production && '
        f'pulumi stack import --file {IMPORT_PATH}'
    )
    print(
        f'  FALLBACK:    gsutil cp {OUTPUT_PATH} '
        f'gs://{GCS_BUCKET}/{args.gcs_blob}'
    )
    print(
        '  The RECOMMENDED path runs Pulumi integrity checks and re-hashes '
        'manifest.magic; the FALLBACK skips both and can surface as an '
        'integrity error on the next `pulumi up`.'
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
