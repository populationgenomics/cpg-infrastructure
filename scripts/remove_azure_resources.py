"""
Strip Azure resources from the production Pulumi stack so cpg-infra can
retire Azure support without a subsequent `pulumi up` provoking destroy diffs
against resources it can no longer authenticate to.

APPROACH
--------
Under --apply, this script invokes `pulumi state remove --force --yes` for
each Azure URN identified in the checkpoint, batched by --batch-size for
throughput. It does NOT edit state JSON directly. --force is required
because non-Azure resources routinely hold references to Azure URNs
(parent / dependencies / provider / etc.); Pulumi would otherwise refuse
to remove those Azure resources.

SAFETY
------
The script makes no calls to any Azure API, requires no Azure credentials,
and never invokes `pulumi up / refresh / destroy`. `pulumi state remove`
mutates only Pulumi's own record of the world -- the resources themselves
remain in Azure until removed out-of-band.

Default is dry-run. The script downloads the checkpoint fresh from GCS on
every invocation (pass --use-cached-backup to reuse an existing local copy;
you rarely want this outside of iteration).

Usage
-----
    # 1. Dry-run against a fresh download (reports what would be removed):
    python scripts/remove_azure_resources.py

    # 2. From inside the Pulumi program directory (contains Pulumi.yaml),
    #    remove every Azure URN via `pulumi state remove --force`:
    python scripts/remove_azure_resources.py --apply \
        --stack datasets/production --pulumi-dir .

    # 3. Verify from the consuming Pulumi program:
    pulumi refresh --preview-only   # should not surface any Azure URNs

The GCS blob path defaults to `.pulumi/stacks/datasets/production.json`; pass
`--gcs-blob <path>` if your stack lives elsewhere in the bucket. --stack
defaults to `datasets/production` and --pulumi-dir to the current directory.

The tradeoff of `pulumi state remove --force`: it deliberately leaves behind
dangling references from surviving non-Azure resources to the removed Azure
URNs. Pulumi will REJECT the next `pulumi up` with a
Snapshot.VerifyIntegrity() error until those references are resolved. The
post-run report at the end of --apply lists every surviving resource that
still needs manual attention.
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

# Explicit non-Azure blocklist so the URN-name heuristic below can never
# mis-classify a well-typed resource as Azure, even if the name happens to
# contain 'azure'. Includes cloud providers, Pulumi's own resource types,
# and the first-party dynamic providers this repo defines.
NON_AZURE_TYPE_PREFIXES = (
    'gcp:',
    'google-native:',
    'pulumi:providers:gcp',
    'pulumi:providers:google-native',
    'aws:',
    'aws-native:',
    'pulumi:providers:aws',
    'pulumi:providers:aws-native',
    'kubernetes:',
    'pulumi:providers:kubernetes',
    'pulumi:providers:pulumi',
    'pulumi:providers:hailbatch',
    'pulumi:providers:metamist',
    'pulumi:providers:seqera',
)

# The URN-name heuristic only fires for types that carry no cloud identity
# in their schema. Any other typed resource is trusted to self-identify via
# AZURE_TYPE_PREFIXES / NON_AZURE_TYPE_PREFIXES / the `azure in type` check
# below -- we DON'T fall back to `-azure-` in the URN name for arbitrarily
# typed components, since a future non-Azure component named
# `dataset-azure-cost-report` would otherwise be swept up along with its
# non-Azure children via leaves-first ordering.
_NAME_HEURISTIC_TYPE_ALLOWLIST = (
    'pulumi:pulumi:Component',
    'pulumi:pulumi:Stack',
)

GCS_BUCKET = 'cpg-pulumi-state'
GCS_BLOB = '.pulumi/stacks/datasets/production.json'
DEFAULT_STACK = 'datasets/production'
DEFAULT_BATCH_SIZE = 50


def is_azure_resource(resource: dict) -> bool:
    resource_type = resource.get('type', '')
    if resource_type.startswith(AZURE_TYPE_PREFIXES):
        return True
    # A well-typed non-Azure resource is never Azure regardless of what its
    # name happens to contain.
    if resource_type.startswith(NON_AZURE_TYPE_PREFIXES):
        return False
    # First-party component types living under an Azure namespace segment
    # (e.g. `cpg_infra:azure:AzureInfra`, `cpg_infra:azure:datasets:...`)
    # self-identify via `:azure:` in their type. Require the colons on both
    # sides so that unrelated-but-Azure-adjacent types like
    # `cpg_infra:reports:AzureCostAudit` (a GCP-side audit of retired Azure
    # spend) or `pulumi:providers:azurerm-mirror` (a proxy shim) don't get
    # swept up along with their non-Azure children.
    if ':azure:' in resource_type.lower():
        return True
    # cpg_infra names component resources without an azure-native/azuread
    # type as `{dataset}-azure-{key}`. Fall back to the URN-name heuristic
    # ONLY for types that don't self-identify at all -- bare Pulumi
    # component/stack types -- so a future non-Azure component doesn't get
    # misclassified by name alone.
    if resource_type not in _NAME_HEURISTIC_TYPE_ALLOWLIST:
        return False
    urn = resource.get('urn', '')
    if not urn:
        return False
    name = urn.rsplit('::', 1)[-1]
    return '-azure-' in name


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
    """Collect refs from fields holding a flat list of URNs / alias entries.

    Aliases can appear either as bare URN strings (legacy form) or as
    alias-spec dicts carrying a `parent` URN field (modern form); both are
    checked.
    """
    for dep in res.get('dependencies') or []:
        if dep in azure_urns:
            refs.append(('dependency', dep))

    for alias in res.get('aliases') or []:
        if isinstance(alias, str):
            if alias in azure_urns:
                refs.append(('alias', alias))
        elif isinstance(alias, dict):
            alias_parent = alias.get('parent')
            if alias_parent and alias_parent in azure_urns:
                refs.append(('alias.parent', alias_parent))


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
    deletedWith, aliases (both string and dict forms). Field-family helpers
    are split out to keep this function's cyclomatic complexity in check.
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
    the `parent` graph restricted to the Azure subset. Removing leaves first
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


def _classify_file_not_found(err: FileNotFoundError, pulumi_dir: str | None) -> str:
    """Return a human-readable error string that distinguishes 'pulumi
    binary not on PATH' from 'the cwd we tried to run in doesn't exist'."""
    if pulumi_dir and getattr(err, 'filename', None) == pulumi_dir:
        return f'--pulumi-dir does not exist: {pulumi_dir}'
    return 'pulumi CLI not found on PATH'


def pulumi_state_remove(
    stack: str,
    urns: list[str],
    pulumi_dir: str | None,
    timeout: int = 300,
) -> tuple[bool, str]:
    """Invoke `pulumi state remove --force --yes --stack <stack> <urn>...`
    with one or more URNs and return (success, captured_output). --force
    acknowledges dangling references (Pulumi warns but proceeds); --yes
    skips the interactive confirm so the loop can run unattended.
    """
    cmd = [
        'pulumi',
        'state',
        'remove',
        '--force',
        '--yes',
        '--stack',
        stack,
        *urns,
    ]
    try:
        # cmd is a fixed argv (no shell); stack + urns come from Pulumi
        # state, not from untrusted input. Safe to call subprocess.run.
        result = subprocess.run(  # noqa: S603
            cmd,
            cwd=pulumi_dir,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as err:
        return False, _classify_file_not_found(err, pulumi_dir)
    except subprocess.TimeoutExpired:
        return False, f'timeout after {timeout}s'
    combined = ((result.stderr or '') + (result.stdout or '')).strip()
    return result.returncode == 0, combined


def _default_backup_path(gcs_blob: str) -> str:
    """Derive the local backup filename from the GCS blob's basename so a
    subsequent invocation against a different stack doesn't collide with an
    earlier stack's cached download."""
    base = os.path.basename(gcs_blob) or 'stack.json'
    stem, ext = os.path.splitext(base)
    return f'{stem}-old{ext or ".json"}'


def load_state(gcs_blob: str, backup_path: str, use_cached: bool) -> dict:
    """Load state from GCS. If use_cached is set and backup_path exists,
    reuse the local copy; otherwise download fresh (default). The default
    always-download behaviour prevents a script re-run from operating on a
    snapshot that has drifted from the live checkpoint (e.g. after another
    engineer's `pulumi up` in the interim).
    """
    if use_cached and os.path.exists(backup_path):
        print(f'Reusing cached backup {backup_path} (--use-cached-backup set)')
        with open(backup_path, encoding='utf-8') as f:
            return json.loads(f.read())

    print(f'Downloading gs://{GCS_BUCKET}/{gcs_blob} -> {backup_path}')
    from google.cloud import storage  # only needed on download

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_blob)
    data_str = blob.download_as_text()

    with open(backup_path, 'w', encoding='utf-8') as f:
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


def _display_string(value: object) -> str | None:
    """Return `value` if it's a non-empty plain string suitable for display,
    else None. Filters out Pulumi's Output marker dicts (recognisable by the
    `4dabf18193072939515e22adb298388d` signature key) and any other non-str
    payload that would render as a garbled dict/None in the report."""
    if isinstance(value, str) and value:
        return value
    return None


def _first_display_string(source: dict, *keys: str) -> str | None:
    """Return the first key's value from `source` that's a display-safe
    string; skip missing keys, empty strings, and Output markers."""
    for key in keys:
        cleaned = _display_string(source.get(key))
        if cleaned is not None:
            return cleaned
    return None


def describe_affected(res: dict) -> dict[str, str]:
    """Extract human-useful identifying info from a Pulumi state resource.

    Merges inputs first, then outputs override -- outputs are always
    concrete resolved values from the provider, while inputs may still
    contain Pulumi's unresolved Output marker dicts that would garble the
    display if they clobbered a resolved bucket/name/project. Values are
    additionally filtered through _display_string so a marker dict that
    only exists in inputs (with no matching outputs key) is dropped rather
    than rendered as `{'4dabf...': ...}` in the operator report.
    """
    inputs = res.get('inputs') or {}
    outputs = res.get('outputs') or {}
    combined = {**inputs, **outputs}

    type_name = res.get('type', '<unknown>')
    # Prefer an explicit bucket field; otherwise, for :Bucket-typed
    # resources, fall back to the `name` field. Both go through
    # _display_string so unresolved Output markers or None never leak into
    # the printed URL.
    bucket = _first_display_string(combined, 'bucket')
    if bucket is None and type_name.endswith(':Bucket'):
        bucket = _first_display_string(combined, 'name')

    obj_name = _first_display_string(combined, 'name')
    project = _first_display_string(combined, 'project')
    location = _first_display_string(combined, 'location', 'region')
    self_link = _first_display_string(combined, 'selfLink', 'url', 'mediaLink')

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


def _looks_like_already_removed(out: str, urn: str) -> bool:
    """Recognise `pulumi state remove` errors that mean 'that URN is not in
    state' -- Pulumi's specific per-URN phrasing is 'No such resource "<urn>"
    exists in the current state'. Match narrowly on that exact substring
    AND require the URN to appear in the output, so unrelated failures
    ('stack not found', 'no project found', 'unknown flag', backend
    resolution errors, etc.) don't get silently rescued as successes."""
    if not out or not urn:
        return False
    lower = out.lower()
    return 'no such resource' in lower and urn.lower() in lower


def _run_batch(
    stack: str,
    batch: list[str],
    pulumi_dir: str | None,
    successes: list[str],
    failures: list[tuple[str, str]],
) -> None:
    """Try the batch as one call; on failure retry per-URN so we can isolate
    which URN(s) actually broke and let the rest through. Per-URN retries
    that hit an "already removed" error are counted as successes since the
    batch call must have removed them before failing on a later URN."""
    ok, out = pulumi_state_remove(stack, batch, pulumi_dir)
    if ok:
        successes.extend(batch)
        return
    first = out.splitlines()[0] if out else '(no output)'
    print(f'    Batch FAILED ({first}); retrying per-URN...')
    for urn in batch:
        ok2, out2 = pulumi_state_remove(stack, [urn], pulumi_dir)
        if ok2 or _looks_like_already_removed(out2, urn):
            successes.append(urn)
        else:
            failures.append((urn, out2))
            first2 = out2.splitlines()[0] if out2 else '(no output)'
            print(f'      FAILED {urn}: {first2}')


def apply_via_pulumi(
    azure: list[dict],
    azure_urns: set[str],
    stack: str,
    pulumi_dir: str | None,
    limit: int | None,
    batch_size: int,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Remove Azure URNs via `pulumi state remove --force --yes` in
    leaves-first, batched calls. Returns (successes, failures). On any batch
    failure, the batch is retried per-URN so failures pinpoint the offending
    URN(s) rather than blaming a whole batch.
    """
    # Pre-validate pulumi_dir so a subsequent FileNotFoundError from
    # subprocess.run unambiguously means the pulumi binary is missing.
    if pulumi_dir is not None and not os.path.isdir(pulumi_dir):
        msg = f'--pulumi-dir does not exist or is not a directory: {pulumi_dir}'
        print(f'ERROR: {msg}', file=sys.stderr)
        return [], [('<pre-flight>', msg)]

    ordered = sort_urns_leaves_first(azure, azure_urns)
    if limit is not None:
        ordered = ordered[:limit]
    total = len(ordered)
    successes: list[str] = []
    failures: list[tuple[str, str]] = []

    for start in range(0, total, batch_size):
        batch = ordered[start : start + batch_size]
        end = start + len(batch)
        print(f'  Batch [{start + 1:>4}-{end:>4}/{total}]: {len(batch)} URN(s)')
        _run_batch(stack, batch, pulumi_dir, successes, failures)

    return successes, failures


def print_post_run_report(
    failures: list[tuple[str, str]],
    problems: list[tuple[str, str, str]],
    azure_pending: list[dict],
    stack: str,
) -> None:
    """Emit the manual-cleanup checklist. --force leaves several loose ends
    the operator MUST address before the next `pulumi up`; this block is the
    single place they're enumerated. `problems` and `azure_pending` here are
    scoped to URNs that were ACTUALLY removed (not the full dry-run set)."""
    print('\n' + '=' * 72)
    print('POST-RUN MANUAL FIXES REQUIRED')
    print('=' * 72)

    step = 1

    if failures:
        print(
            f'\n{step}. {len(failures)} URN(s) failed to remove. Inspect the '
            f'errors above and either re-run this script (successful '
            f'removals are idempotent for URNs already absent) or handle '
            f'each URN individually with `pulumi state remove --force --yes '
            f'--stack {stack} <urn>`.'
        )
        step += 1

    if problems:
        distinct_dependents = sorted({urn for urn, _, _ in problems})
        print(
            f'\n{step}. {len(distinct_dependents)} surviving non-Azure '
            f'resource(s) hold dangling references to Azure URNs that were '
            f"successfully removed. Pulumi's Snapshot.VerifyIntegrity() "
            f'will REJECT the next `pulumi up` until these are cleared. '
            f'Options, in order of preference:'
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
            f'Azure URNs that were removed. `pulumi state remove` clears the '
            f'resource but does not always drop related pending ops. If '
            f'`pulumi up` complains about pending operations on removed '
            f'URNs, run `pulumi cancel --stack {stack}`.'
        )
        step += 1

    print(
        f'\n{step}. Verify: `pulumi refresh --preview-only --stack {stack}` '
        f'should surface no Azure URNs. If it does, they were skipped by '
        f'--limit or listed in the failures above.'
    )

    if not (failures or problems or azure_pending):
        print(
            '\n(No manual fixes required beyond the standard `pulumi '
            'refresh --preview` verification -- no dangling references, '
            'no failures, no pending Azure ops.)'
        )


def _build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Invoke `pulumi state remove --force --yes` for each Azure '
        'URN. Without this flag, the script only reports what would be '
        'removed and which cross-cloud edges would be left dangling.',
    )
    parser.add_argument(
        '--gcs-blob',
        default=GCS_BLOB,
        help=f'Blob path within gs://{GCS_BUCKET}/ to download. '
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
        help='If set, remove only the first N Azure URNs (leaves-first '
        'order). Useful for staged rollouts or smoke tests.',
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f'Number of URNs per `pulumi state remove` invocation. Higher '
        f'is faster but on failure a full batch is retried per-URN to '
        f'isolate the culprit. Use 1 to force strict per-URN removals. '
        f'Default: {DEFAULT_BATCH_SIZE}',
    )
    parser.add_argument(
        '--backup-path',
        default=None,
        help='Where to write the downloaded checkpoint locally. Defaults '
        'to `<gcs-blob-basename>-old.json` in the current directory, so '
        'different stacks/blobs get different backup files.',
    )
    parser.add_argument(
        '--use-cached-backup',
        action='store_true',
        help='Reuse an existing local backup file instead of re-downloading '
        'from GCS. Default is to always download fresh so the script never '
        'operates on a snapshot that has drifted from live state. Only set '
        'this for tight iteration on the report itself.',
    )
    return parser


def _collect_azure(
    resources: list[dict],
) -> tuple[list[dict], list[dict], set[str]]:
    """Partition resources into (azure, non_azure) and collect the set of
    Azure URNs. Rows without a `urn` key are dropped from `azure` entirely
    so an empty string never poisons downstream membership tests or gets
    passed as a positional to `pulumi state remove`."""
    azure: list[dict] = []
    non_azure: list[dict] = []
    for r in resources:
        if is_azure_resource(r):
            if r.get('urn'):
                azure.append(r)
        else:
            non_azure.append(r)
    azure_urns = {r['urn'] for r in azure}
    return azure, non_azure, azure_urns


def _print_dry_run_findings(
    azure_urns: set[str],
    non_azure: list[dict],
    azure_pending: list[dict],
) -> list[tuple[str, str, str]]:
    """Print the pre-flight scan (pending ops + full edge list). Does NOT
    call report_affected -- the caller renders that with the appropriate
    scope (full set for dry-run; only successfully-removed URNs post-apply)
    so the survivor detail block always matches the numbers around it."""
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
            f'\n{len(problems)} dangling reference edge(s) would remain '
            f'if all Azure URNs are removed: {len(distinct_dependents)} '
            f'surviving resource(s) reference {len(distinct_targets)} '
            f'Azure URN(s) via parent / dependencies / propertyDependencies '
            f'/ provider / providers / deletedWith / aliases. `pulumi state '
            f'remove --force` leaves these edges in place; they must be '
            f'resolved manually before the next `pulumi up`. Full edge list:'
        )
        for dep_urn, kind, azure_urn in problems:
            print(f'  {dep_urn}\n    {kind} -> {azure_urn}')
    return problems


def main() -> int:
    args = _build_argparser().parse_args()

    backup_path = args.backup_path or _default_backup_path(args.gcs_blob)
    data = load_state(args.gcs_blob, backup_path, args.use_cached_backup)
    latest = data['checkpoint']['latest']
    resources = latest['resources']

    azure, non_azure, azure_urns = _collect_azure(resources)

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
    problems = _print_dry_run_findings(azure_urns, non_azure, azure_pending)

    if not args.apply:
        # Dry-run: survivor detail is scoped to the full pre-flight problem
        # set since we're previewing the full removal.
        if problems:
            report_affected(problems, non_azure)
        print(
            f'\nDry-run only. Re-run with --apply to invoke '
            f'`pulumi state remove --force --yes --stack {args.stack}` '
            f'in batches of {args.batch_size} (leaves first).'
        )
        return 0

    print(
        f'\nRemoving {len(azure)} Azure URN(s) from stack {args.stack} '
        f'via `pulumi state remove --force --yes` (leaves first, '
        f'batches of {args.batch_size})...'
    )
    successes, failures = apply_via_pulumi(
        azure,
        azure_urns,
        args.stack,
        args.pulumi_dir,
        args.limit,
        args.batch_size,
    )
    print(f'\nRemoval summary: {len(successes)} succeeded, ' f'{len(failures)} failed.')

    # Post-run reporting must reflect what ACTUALLY happened, not the
    # dry-run scope: under --limit or partial failure, most surviving-side
    # "problems" from the dry-run aren't dangling because their targets
    # weren't removed. Recompute using only successfully-removed URNs.
    removed_urns = set(successes)
    actual_problems = find_cross_cloud_refs(removed_urns, non_azure)
    actual_pending = [
        op for op in pending if (op.get('resource') or {}).get('urn') in removed_urns
    ]

    # Render the survivor detail scoped to what was actually removed, so the
    # post-run checklist's "list above" reference is the correct list.
    if actual_problems:
        report_affected(actual_problems, non_azure)

    print_post_run_report(
        failures,
        actual_problems,
        actual_pending,
        args.stack,
    )
    # Non-zero exit on any failure so CI wrappers don't mark a partial run
    # as green. `problems` here is intentionally the pre-flight set; we only
    # gate exit on subprocess failures, not on dangling-refs (those are
    # expected under --force and require operator judgement, not CI retry).
    _ = problems
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
