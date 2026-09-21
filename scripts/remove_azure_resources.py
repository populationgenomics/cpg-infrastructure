"""Strip Azure resources from a Pulumi stack.

Reports the Azure URNs to remove and, for every non-Azure survivor that still
references one, prints the affected URN and reference fields. It does not
generate an automatic survivor-delete command because that can remove the
survivor and its descendants from state.

With `--apply`, iterates the URNs leaves-first and shells out
`pulumi state remove --force --yes` once per URN, tallying successes and
failures. Default is dry-run.

State is read from `pulumi stack export --stack <stack>` by default, or from
a local JSON dump via `--state-file` (accepts both a `pulumi stack export`
output and a raw backend checkpoint).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

DEFAULT_STACK = 'organization/datasets/production'

AZURE_TYPE_PREFIXES = (
    'azure-native:',
    'azuread:',
    'pulumi:providers:azure-native',
    'pulumi:providers:azuread',
)


def is_azure_resource(resource: dict) -> bool:
    # Classify from the type alone: Azure providers always self-identify with
    # a type prefix, and first-party component types put `:azure:` in the
    # type namespace. A URN-name check would misclassify well-typed non-Azure
    # resources whose names happen to contain `azure` (e.g. a GCP bucket
    # object named `storage-config-azure-common`).
    resource_type = resource.get('type', '')
    return (
        resource_type.startswith(AZURE_TYPE_PREFIXES)
        or ':azure:' in resource_type.lower()
    )


def _extract_provider_urn(provider_ref: str) -> str:
    """Extract URN from `<urn>::<id>` provider reference format."""
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
    """Collect refs from dependencies, aliases (string and dict forms)."""
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
    """Return (dependent_urn, ref_kind, azure_urn) for every non-Azure resource
    that references an Azure URN. `pulumi state remove --force` leaves these
    edges behind; they must be cleaned manually before the next `pulumi up`."""
    problems: list[tuple[str, str, str]] = []
    for res in non_azure:
        urn = res.get('urn')
        if not urn:
            continue
        refs: list[tuple[str, str]] = []
        _add_single_field_refs(res, azure_urns, refs)
        _add_list_field_refs(res, azure_urns, refs)
        _add_property_dependency_refs(res, azure_urns, refs)
        _add_provider_map_refs(res, azure_urns, refs)
        for kind, target in refs:
            problems.append((urn, kind, target))
    return problems


def build_manual_commands(
    problems: list[tuple[str, str, str]],
) -> list[str]:
    """Return non-executable report lines for each surviving resource.

    Do not generate `pulumi state delete --target-dependents` commands here:
    that command removes the survivor and its descendants, rather than just
    clearing the dangling Azure reference.
    """
    kinds_by_urn: dict[str, set[str]] = {}
    for dep_urn, kind, _ in problems:
        kinds_by_urn.setdefault(dep_urn, set()).add(kind)

    lines: list[str] = []
    for urn in sorted(kinds_by_urn):
        kinds = ', '.join(sorted(kinds_by_urn[urn]))
        lines.append(f'# {urn}  (refs: {kinds})')
    return lines


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--stack', default=DEFAULT_STACK)
    parser.add_argument('--pulumi-dir', default=None)
    parser.add_argument(
        '--state-file',
        default=None,
        help='Read state from this JSON file (produced by `pulumi stack '
        'export`) instead of shelling out. Dry-run only; --apply still '
        'requires a working `pulumi` login for `pulumi state remove`.',
    )
    return parser.parse_args()


def load_state_from_file(path: str) -> dict:
    """Return the parsed state from a local `pulumi stack export` dump."""
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f'ERROR: --state-file not found: {path}', file=sys.stderr)
        sys.exit(2)
    except json.JSONDecodeError as err:
        print(f'ERROR: --state-file is not valid JSON ({err}): {path}', file=sys.stderr)
        sys.exit(2)


def load_state(stack: str, pulumi_dir: str | None) -> dict:
    """Return the parsed output of `pulumi stack export --stack <stack>`.

    On failure, print pulumi's stderr (or a targeted hint for missing binary /
    non-JSON stdout) and exit non-zero instead of raising an opaque traceback.
    """
    try:
        result = subprocess.run(  # noqa: S603
            ['pulumi', 'stack', 'export', '--stack', stack],  # noqa: S607
            cwd=pulumi_dir,
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError:
        print(
            'ERROR: `pulumi` not on PATH. Install the Pulumi CLI '
            '(https://www.pulumi.com/docs/install/).',
            file=sys.stderr,
        )
        sys.exit(127)
    except subprocess.CalledProcessError as err:
        print(
            f'ERROR: `pulumi stack export --stack {stack}` failed '
            f'(exit {err.returncode}). Run from a Pulumi program dir or pass '
            f"--pulumi-dir; verify `pulumi login` and stack access. For CPG's "
            f'self-managed backend the stack must be fully qualified as '
            f'`organization/<project>/<stack>` (org literal is `organization`).\n'
            f'stderr:\n{err.stderr}',
            file=sys.stderr,
        )
        sys.exit(err.returncode)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as err:
        print(
            f'ERROR: `pulumi stack export` produced non-JSON output '
            f'({err}). First 500 bytes of stdout:\n{result.stdout[:500]}',
            file=sys.stderr,
        )
        sys.exit(2)


def leaves_first_order(urns_to_remove: set[str], resources: list[dict]) -> list[str]:
    """Order URNs so children come before their parents. Only parent edges
    within the removal set count; ties broken alphabetically for determinism."""
    parent_of = {r['urn']: r.get('parent') for r in resources if r.get('urn')}

    def depth(urn: str) -> int:
        d = 0
        cur = parent_of.get(urn)
        while cur in urns_to_remove:
            d += 1
            cur = parent_of.get(cur)
        return d

    return sorted(urns_to_remove, key=lambda u: (-depth(u), u))


def pulumi_state_remove_one(stack: str, urn: str, pulumi_dir: str | None) -> None:
    """`pulumi state remove --force --yes --stack <stack> <urn>` for a single URN.

    --force lets Pulumi proceed even when non-Azure survivors still hold
    references to the removed URN (those refs are what the manual-command
    block above is for). --yes skips the interactive prompt.

    Raises CalledProcessError on non-zero exit -- the caller reports it.
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
    subprocess.run(cmd, cwd=pulumi_dir, check=True)  # noqa: S603


def _load_resources(args: argparse.Namespace) -> list[dict]:
    """Load state from --state-file or the live stack and return its resources."""
    if args.state_file:
        state = load_state_from_file(args.state_file)
    else:
        state = load_state(args.stack, args.pulumi_dir)
    # `pulumi stack export` wraps resources in `deployment.resources`; a raw
    # backend checkpoint (e.g. downloaded straight from GCS) wraps them in
    # `checkpoint.latest.resources`. Accept either.
    resources = (
        state.get('deployment', {}).get('resources')
        or state.get('checkpoint', {}).get('latest', {}).get('resources')
        or []
    )
    if not resources:
        source = (
            f'--state-file {args.state_file}'
            if args.state_file
            else f'stack {args.stack!r}'
        )
        print(
            f'ERROR: no resources found in {source} '
            f'(empty stack, wrong stack, or unrecognised export layout: '
            f'expected `deployment.resources` or `checkpoint.latest.resources`).',
            file=sys.stderr,
        )
        sys.exit(2)
    return resources


def _print_survivor_block(commands: list[str], pulumi_dir: str | None) -> None:
    """Print the report for surviving non-Azure cross-cloud references."""
    if not commands:
        print('\nNo surviving non-Azure resources reference Azure URNs.')
        return
    survivor_count = sum(1 for line in commands if line.startswith('# '))
    print(
        f'\nAfter `pulumi state remove`, {survivor_count} non-Azure '
        f'resource(s) will hold dangling references to removed Azure '
        f'URNs. Review the affected resources below before repairing state '
        f'from {pulumi_dir or "."}.'
    )
    print(
        '\n# No automatic delete command is provided: '
        '`pulumi state delete --target-dependents` removes the survivor '
        'and its descendants from state. Repair only the listed dangling '
        'fields using a reviewed state edit or a targeted Pulumi workflow; '
        'then verify with `pulumi preview`.'
    )
    print('\n'.join(commands))


def _apply_removals(
    azure_urns: set[str],
    resources: list[dict],
    stack: str,
    pulumi_dir: str | None,
    has_survivor_block: bool,
) -> int:
    """Run `pulumi state remove` per URN leaves-first; return exit code."""
    if not azure_urns:
        print('\nNothing to remove.')
        return 0
    ordered = leaves_first_order(azure_urns, resources)
    print(f'\nRemoving {len(ordered)} URN(s) leaves-first...')
    succeeded: list[str] = []
    failed: list[tuple[str, int]] = []
    for urn in ordered:
        try:
            pulumi_state_remove_one(stack, urn, pulumi_dir)
            succeeded.append(urn)
        except subprocess.CalledProcessError as err:
            failed.append((urn, err.returncode))
            print(f'  FAILED (exit {err.returncode}): {urn}', file=sys.stderr)
    print(f'\nDone. {len(succeeded)} removed, {len(failed)} failed.')
    if failed:
        print('Failed URNs:', file=sys.stderr)
        for urn, rc in failed:
            print(f'  (exit {rc}) {urn}', file=sys.stderr)
    if succeeded and has_survivor_block:
        print('Run the manual-command block above.')
    return 1 if failed else 0


def main() -> int:
    args = parse_args()
    if args.state_file and args.apply:
        print(
            'ERROR: --state-file is dry-run only; drop --apply or point '
            'the script at a live stack.',
            file=sys.stderr,
        )
        return 2
    resources = _load_resources(args)
    azure = [r for r in resources if is_azure_resource(r) and r.get('urn')]
    non_azure = [r for r in resources if not is_azure_resource(r)]
    azure_urns = {r['urn'] for r in azure}
    print(f'Azure URNs to remove: {len(azure_urns)}')
    for urn in sorted(azure_urns):
        print(f'  {urn}')
    commands = build_manual_commands(find_cross_cloud_refs(azure_urns, non_azure))
    _print_survivor_block(commands, args.pulumi_dir)
    if not args.apply:
        print(
            f'\nDry-run only. Re-run with --apply to invoke '
            f'`pulumi state remove --force --yes --stack {args.stack}`.'
        )
        return 0
    return _apply_removals(
        azure_urns,
        resources,
        args.stack,
        args.pulumi_dir,
        has_survivor_block=bool(commands),
    )


if __name__ == '__main__':
    sys.exit(main())
