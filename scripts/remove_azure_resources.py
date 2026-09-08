"""
Strip Azure resources from the production Pulumi stack so cpg-infra can
retire Azure support without a subsequent `pulumi up` provoking destroy diffs
against resources it can no longer authenticate to.

APPROACH
--------
Under --apply, this script invokes `pulumi state remove --force --yes` for
each Azure URN identified in the checkpoint. It does NOT edit state JSON
directly (see NOTE below). --force is required because non-Azure resources
routinely hold references to Azure URNs (parent / dependencies / provider /
etc.); Pulumi would otherwise refuse to remove those Azure resources.

SAFETY
------
The script makes no calls to any Azure API, requires no Azure credentials,
and never invokes `pulumi up / refresh / destroy`. `pulumi state remove`
mutates only Pulumi's own record of the world -- the resources themselves
remain in Azure until removed out-of-band.

Default is dry-run. Under dry-run the script downloads the checkpoint from
GCS purely to enumerate what would be removed and which cross-cloud edges
would be left dangling.

Usage
-----
    # 1. Dry-run against a fresh download (reports what would be removed):
    python scripts/remove_azure_resources.py

    # 2. From inside the Pulumi program directory (contains Pulumi.yaml),
    #    remove every Azure URN via `pulumi state remove --force`:
    python scripts/remove_azure_resources.py --apply \
        --stack datasets/production --pulumi-dir .

    # 3. Verify from the consuming Pulumi program:
    pulumi refresh --preview   # should not surface any Azure URNs

The GCS blob path defaults to `.pulumi/stacks/datasets/production.json`; pass
`--gcs-blob <path>` if your stack lives elsewhere in the bucket. --stack
defaults to `datasets/production` and --pulumi-dir to the current directory.

NOTE: We intentionally do NOT edit the state JSON directly, and we do NOT
use `pulumi stack import` -- both have burned this codebase (JSON syntax
corruption / broken dependency trees). `pulumi state remove --force` is the
supported Pulumi command for removing a resource from state without touching
the underlying cloud, and it correctly maintains manifest.magic, integrity
metadata, and Pulumi's local backup chain in `.pulumi/backups/`.

The tradeoff: `--force` deliberately leaves behind dangling references from
surviving non-Azure resources to the deleted Azure URNs. Pulumi will REJECT
the next `pulumi up` with a Snapshot.VerifyIntegrity() error until those
references are resolved. The post-run report at the end of --apply lists
every surviving resource that still needs manual attention; see
"POST-RUN MANUAL FIXES REQUIRED" in the output.
"""

import argparse
import json
import os
import subprocess
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
DEFAULT_STACK = 'datasets/production'


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


def _add_single_field_refs(
    res: dict, azure_urns: set[str], refs: list[tuple[str, str]]
) -> None:
    """Collect refs from fields holding a single URN or provider reference."""
    parent = res.get('parent')
    if parent and parent in azure_urns:
        refs.append(('parent', parent))

    deleted_with = res.get('deletedWith')
    if deleted_with and deleted_with in azure_urns:
        refs.append(('deletedWith', deleted_with))

    provider = res.get('provider')
    if provider and _extract_provider_urn(provider) in azure_urns:
        refs.append(('provider', provider))


def _add_list_field_refs(
    res: dict, azure_urns: set[str], refs: list[tuple[str, str]]
) -> None:
    """Collect refs from fields holding a flat list of URNs / alias entries."""
    for dep in res.get('dependencies') or []:
        if dep in azure_urns:
            refs.append(('dependency', dep))

    for alias in res.get('aliases') or []:
        if isinstance(alias, str) and alias in azure_urns:
            refs.append(('alias', alias))


def _add_property_dependency_refs(
    res: dict, azure_urns: set[str], refs: list[tuple[str, str]]
) -> None:
    """Collect refs from `propertyDependencies` (map of property -> URN list)."""
    for prop, deps in (res.get('propertyDependencies') or {}).items():
        for dep in deps or []:
            if dep in azure_urns:
                refs.append((f'propertyDependency[{prop}]', dep))


def _add_provider_map_refs(
    res: dict, azure_urns: set[str], refs: list[tuple[str, str]]
) -> None:
    """Collect refs from `providers` (map of package -> provider reference)."""
    for pkg, prov_ref in (res.get('providers') or {}).items():
        if prov_ref and _extract_provider_urn(prov_ref) in azure_urns:
            refs.append((f'providers[{pkg}]', prov_ref))


def find_cross_cloud_refs(
    azure_urns: set[str], non_azure: list[dict]
) -> list[tuple[str, str, str]]:
    """Return (dependent_urn, ref_kind, azure_urn) for every non-Azure
    resource that references an Azure URN.

    These edges are what `pulumi state remove --force` deliberately leaves
    behind: Pulumi removes the target Azure resource from state but does NOT
    walk back and prune the pointers held by survivors. Every edge reported
    here becomes a manual cleanup task before the next `pulumi up`.

    Covers every field Pulumi uses to point at another resource's URN:
    parent, dependencies, propertyDependencies, provider, providers,
    deletedWith, aliases. Field-family helpers are split out to keep this
    function's cyclomatic complexity in check.
    """
    problems: list[tuple[str, str, str]] = []
    for res in non_azure:
        urn = res.get('urn', '<unknown>')
        refs: list[tuple[str, str]] = []
        _add_single_field_refs(res, azure_urns, refs)
        _add_list_field_refs(res, azure_urns, refs)
        _add_property_dependency_refs(res, azure_urns, refs)
        _add_provider_map_refs(res, azure_urns, refs)
        for kind, target in refs:
            problems.append((urn, kind, target))
    return problems


def sort_urns_leaves_first(azure: list[dict], azure_urns: set[str]) -> list[str]:
    """Return Azure URNs in leaf-first order (children before parents) using
    the `parent` graph restricted to the Azure subset. Deleting leaves first
    minimises Pulumi's warnings about outstanding child references and keeps
    each `pulumi state remove --force` call closer to a "clean" removal.
    """
    parent_of: dict[str, str | None] = {}
    for r in azure:
        u = r.get('urn')
        if not u:
            continue
        p = r.get('parent')
        parent_of[u] = p if p in azure_urns else None

    children_of: dict[str, list[str]] = {u: [] for u in parent_of}
    for u, p in parent_of.items():
        if p and p in children_of:
            children_of[p].append(u)

    order: list[str] = []
    visited: set[str] = set()

    def visit(u: str) -> None:
        if u in visited:
            return
        visited.add(u)
        for child in children_of.get(u, []):
            visit(child)
        order.append(u)

    for u in parent_of:
        visit(u)
    return order


def pulumi_state_remove(
    stack: str, urn: str, pulumi_dir: str | None
) -> tuple[bool, str]:
    """Invoke `pulumi state remove --force --yes --stack <stack> <urn>` and
    return (success, captured_output). --force acknowledges dangling
    references (Pulumi warns but proceeds); --yes skips the interactive
    confirm so the loop can run unattended.
    """
    cmd = [
        'pulumi',
        'state',
        'remove',
        '--force',
        '--yes',
        '--stack',
        stack,
        urn,
    ]
    try:
        # cmd is a fixed argv (no shell); stack + urn come from Pulumi state,
        # not from untrusted input. Safe to call subprocess.run directly.
        result = subprocess.run(  # noqa: S603
            cmd,
            cwd=pulumi_dir,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except FileNotFoundError:
        return False, 'pulumi CLI not found on PATH'
    except subprocess.TimeoutExpired:
        return False, 'timeout after 120s'
    combined = ((result.stderr or '') + (result.stdout or '')).strip()
    return result.returncode == 0, combined


def load_state(gcs_blob: str) -> dict:
    """Load state from local backup if present; otherwise download from GCS.

    The download is purely for enumeration -- the script never uploads state
    back. `.pulumi/backups/` under the Pulumi program directory is the
    authoritative rollback path if a `pulumi state remove` run needs undoing.
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
    detailed listing so the operator can see exactly which resources will
    hold a dangling Azure URN reference after --apply."""
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


def apply_via_pulumi(
    azure: list[dict],
    azure_urns: set[str],
    stack: str,
    pulumi_dir: str | None,
    limit: int | None,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Run `pulumi state remove --force --yes` for each Azure URN in
    leaves-first order. Returns (successes, failures). Failures are captured
    with their combined stdout/stderr so the operator can triage individual
    URNs without re-running the whole batch."""
    ordered = sort_urns_leaves_first(azure, azure_urns)
    if limit is not None:
        ordered = ordered[:limit]
    total = len(ordered)
    successes: list[str] = []
    failures: list[tuple[str, str]] = []
    for i, urn in enumerate(ordered, 1):
        print(f'  [{i:>4}/{total}] {urn}')
        ok, out = pulumi_state_remove(stack, urn, pulumi_dir)
        if ok:
            successes.append(urn)
        else:
            failures.append((urn, out))
            first = out.splitlines()[0] if out else '(no output)'
            print(f'    FAILED: {first}')
    return successes, failures


def print_post_run_report(
    failures: list[tuple[str, str]],
    problems: list[tuple[str, str, str]],
    azure_pending: list[dict],
    stack: str,
) -> None:
    """Emit the manual-cleanup checklist. --force leaves several loose ends
    the operator MUST address before the next `pulumi up`; this block is the
    single place they're enumerated."""
    print('\n' + '=' * 72)
    print('POST-RUN MANUAL FIXES REQUIRED')
    print('=' * 72)

    step = 1

    if failures:
        print(
            f'\n{step}. {len(failures)} URN(s) failed to delete. Inspect the '
            f'errors above and either re-run this script (successful '
            f'deletes are idempotent for URNs already absent) or handle '
            f'each URN individually with `pulumi state remove --force --yes '
            f'--stack {stack} <urn>`.'
        )
        step += 1

    if problems:
        distinct_dependents = sorted({urn for urn, _, _ in problems})
        print(
            f'\n{step}. {len(distinct_dependents)} surviving non-Azure '
            f'resource(s) hold dangling references to the now-deleted Azure '
            f"URNs. Pulumi's Snapshot.VerifyIntegrity() will REJECT the "
            f'next `pulumi up` until these are cleared. Options, in order '
            f'of preference:'
        )
        print(
            '     (a) For each surviving URN, use targeted `pulumi state` '
            'subcommands to detach the reference where possible '
            '(`pulumi state unprotect`, `pulumi state rename`, or '
            '`pulumi state remove --target-dependents` when the survivor '
            'itself is disposable).'
        )
        print(
            '     (b) `pulumi refresh --disable-integrity-checking` once, '
            'then let `pulumi up` re-serialize the state without the '
            'broken edges. Verify the diff carefully before confirming.'
        )
        print(
            '     (c) As a last resort, hand-edit the surviving resources '
            'in state to drop the dangling parent / dependencies / '
            'provider / providers / deletedWith / aliases entries. Scope '
            f'is bounded -- only {len(distinct_dependents)} resource(s) '
            'need touching, not the full Azure set.'
        )
        print(
            '     The full list of affected survivors is printed above '
            'under "Affected surviving resources (full detail)".'
        )
        step += 1

    if azure_pending:
        print(
            f'\n{step}. {len(azure_pending)} pending operation(s) referenced '
            f'Azure URNs at download time. `pulumi state remove` clears the '
            f'resource but does not always drop related pending ops. If '
            f'`pulumi up` complains about pending operations on removed '
            f'URNs, run `pulumi cancel --stack {stack}`.'
        )
        step += 1

    print(
        f'\n{step}. Verify: `pulumi refresh --preview --stack {stack}` '
        f'should surface no Azure URNs. If it does, they were skipped by '
        f'--limit or listed in the failures above.'
    )

    if not (failures or problems or azure_pending):
        print(
            '\n(No manual fixes required beyond the standard `pulumi '
            'refresh --preview` verification -- no dangling references, '
            'no failures, no pending Azure ops.)'
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Invoke `pulumi state remove --force --yes` for each Azure '
        'URN. Without this flag, the script only reports what would be '
        'deleted and which cross-cloud edges would be left dangling.',
    )
    parser.add_argument(
        '--gcs-blob',
        default=GCS_BLOB,
        help=f'Blob path within gs://{GCS_BUCKET}/ to download when '
        f'{BACKUP_PATH} is not already present locally. '
        f'Default: {GCS_BLOB}',
    )
    parser.add_argument(
        '--stack',
        default=DEFAULT_STACK,
        help=f'Pulumi stack name passed via --stack to each '
        f'`pulumi state remove` call. Default: {DEFAULT_STACK}',
    )
    parser.add_argument(
        '--pulumi-dir',
        default=None,
        help='Working directory for the `pulumi` subprocess. Must contain '
        'Pulumi.yaml for the target project. Defaults to the current '
        'directory.',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='If set, delete only the first N Azure URNs (leaves-first '
        'order). Useful for staged rollouts or smoke tests.',
    )
    args = parser.parse_args()

    data = load_state(args.gcs_blob)
    latest = data['checkpoint']['latest']
    resources = latest['resources']

    azure = [r for r in resources if is_azure_resource(r)]
    non_azure = [r for r in resources if not is_azure_resource(r)]
    azure_urns = {r.get('urn', '') for r in azure}

    # `pending_operations` records in-flight create/update/delete ops from an
    # interrupted `pulumi up`. `pulumi state remove` does not always drop
    # related pending ops, so we surface any Azure ones so the operator can
    # follow up with `pulumi cancel` if needed.
    pending = latest.get('pending_operations') or []
    azure_pending = [
        op for op in pending if (op.get('resource') or {}).get('urn') in azure_urns
    ]

    print(f'\nLoaded {len(resources)} resources.')
    print(f'  Azure resources to remove: {len(azure)}')
    print(f'  Non-Azure resources kept:  {len(non_azure)}')
    if pending:
        print(f'  Pending operations total:  {len(pending)}')
        print(f'    Azure pending in scope:  {len(azure_pending)}')

    if not azure:
        print('\nNo Azure resources found. Nothing to do.')
        return 0

    summarise(azure)

    if azure_pending:
        print('\nAzure pending operations to review post-run:')
        for op in azure_pending:
            op_type = op.get('type', '<unknown>')
            urn = (op.get('resource') or {}).get('urn', '<unknown>')
            print(f'  {op_type:>8}  {urn}')

    problems = find_cross_cloud_refs(azure_urns, non_azure)
    if problems:
        distinct_dependents = {urn for urn, _, _ in problems}
        distinct_targets = {target for _, _, target in problems}
        print(
            f'\n{len(problems)} dangling reference edge(s) will remain '
            f'after --apply: {len(distinct_dependents)} surviving '
            f'resource(s) reference {len(distinct_targets)} Azure URN(s) '
            'via parent / dependencies / propertyDependencies / provider / '
            'providers / deletedWith / aliases. `pulumi state remove '
            '--force` deliberately leaves these edges in place; they must '
            'be resolved manually before the next `pulumi up`. Full edge '
            'list:'
        )
        for dep_urn, kind, azure_urn in problems:
            print(f'  {dep_urn}\n    {kind} -> {azure_urn}')

        report_affected(problems, non_azure)

    if not args.apply:
        print(
            f'\nDry-run only. Re-run with --apply to invoke '
            f'`pulumi state remove --force --yes --stack {args.stack} <urn>` '
            f'for each Azure URN (leaves first).'
        )
        return 0

    print(
        f'\nDeleting {len(azure)} Azure URN(s) from stack {args.stack} '
        f'via `pulumi state remove --force --yes` (leaves first)...'
    )
    successes, failures = apply_via_pulumi(
        azure,
        azure_urns,
        args.stack,
        args.pulumi_dir,
        args.limit,
    )
    print(
        f'\nDeletion summary: {len(successes)} succeeded, ' f'{len(failures)} failed.'
    )

    print_post_run_report(
        failures,
        problems,
        azure_pending,
        args.stack,
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
