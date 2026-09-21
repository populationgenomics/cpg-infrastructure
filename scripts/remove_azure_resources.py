"""Strip Azure resources from a Pulumi stack.

Reports URNs to remove, (with --apply) issues one `pulumi state remove --force --yes`
call, and prints manual `pulumi state delete` commands for non-Azure survivors that
reference removed Azure URNs. Default is dry-run. State via `pulumi stack export`.
"""

from __future__ import annotations
import argparse
import json
import shlex
import subprocess
import sys

DEFAULT_STACK = 'datasets/production'

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
    problems: list[tuple[str, str, str]], stack: str
) -> list[str]:
    """Return shell lines, two per surviving URN: a `#`-comment listing ref
    kinds, then a `pulumi state delete --target-dependents` command. URNs are
    shell-quoted via shlex.quote()."""
    kinds_by_urn: dict[str, set[str]] = {}
    for dep_urn, kind, _ in problems:
        kinds_by_urn.setdefault(dep_urn, set()).add(kind)

    lines: list[str] = []
    for urn in sorted(kinds_by_urn):
        kinds = ', '.join(sorted(kinds_by_urn[urn]))
        lines.append(f'# {urn}  (refs: {kinds})')
        lines.append(
            f"pulumi state delete --target-dependents --force --yes "
            f"--stack {shlex.quote(stack)} {shlex.quote(urn)}"
        )
    return lines


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--stack', default=DEFAULT_STACK)
    parser.add_argument('--pulumi-dir', default=None)
    return parser.parse_args()


def load_state(stack: str, pulumi_dir: str | None) -> dict:
    """Return the parsed output of `pulumi stack export --stack <stack>`.

    On failure, print pulumi's stderr (or a targeted hint for missing binary /
    non-JSON stdout) and exit non-zero instead of raising an opaque traceback.
    """
    try:
        result = subprocess.run(  # noqa: S603
            ['pulumi', 'stack', 'export', '--stack', stack],
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
            f'--pulumi-dir; verify `pulumi login` and stack access.\n'
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


def leaves_first_order(
    urns_to_remove: set[str], resources: list[dict]
) -> list[str]:
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


def pulumi_state_remove_one(
    stack: str, urn: str, pulumi_dir: str | None
) -> None:
    """`pulumi state remove --force --yes --stack <stack> <urn>` for a single URN.

    --force lets Pulumi proceed even when non-Azure survivors still hold
    references to the removed URN (those refs are what the manual-command
    block above is for). --yes skips the interactive prompt.

    Raises CalledProcessError on non-zero exit -- the caller reports it.
    """
    cmd = [
        'pulumi', 'state', 'remove',
        '--force', '--yes',
        '--stack', stack,
        urn,
    ]
    subprocess.run(cmd, cwd=pulumi_dir, check=True)  # noqa: S603


def main() -> int:
    args = parse_args()
    state = load_state(args.stack, args.pulumi_dir)
    resources = state.get('deployment', {}).get('resources') or []
    if not resources:
        print(
            f'ERROR: stack {args.stack!r} has no resources in export '
            f'(empty stack, wrong stack, or export format changed).',
            file=sys.stderr,
        )
        return 2
    azure = [r for r in resources if is_azure_resource(r) and r.get('urn')]
    non_azure = [r for r in resources if not is_azure_resource(r)]
    azure_urns = {r['urn'] for r in azure}
    print(f'Azure URNs to remove: {len(azure_urns)}')
    for urn in sorted(azure_urns):
        print(f'  {urn}')
    problems = find_cross_cloud_refs(azure_urns, non_azure)
    commands = build_manual_commands(problems, args.stack)
    if commands:
        survivor_count = sum(1 for line in commands if line.startswith('# '))
        print(
            f'\nAfter `pulumi state remove`, {survivor_count} non-Azure '
            f'resource(s) will hold dangling references to removed Azure '
            f'URNs. Review the block below and paste it into the PR summary; '
            f'run each command from {args.pulumi_dir or "."}.'
        )
        print(
            '\n# WARNING: `pulumi state delete --target-dependents` removes '
            'the survivor AND every resource under it from state. Only run it '
            'as-is when the survivor is disposable. For load-bearing '
            'survivors, prefer:\n'
            '#   (a) narrower `pulumi state` commands (unprotect / rename) '
            'that detach only the dangling edge,\n'
            '#   (b) `pulumi refresh --disable-integrity-checking` once, '
            'then let `pulumi up` re-serialize state without the broken edges,\n'
            '#   (c) hand-edit the surviving resource in state to drop the '
            'dangling parent / dependencies / provider / providers / '
            'deletedWith / aliases entry.'
        )
        print('\n'.join(commands))
    else:
        print('\nNo surviving non-Azure resources reference Azure URNs.')

    if not args.apply:
        print(
            f'\nDry-run only. Re-run with --apply to invoke '
            f'`pulumi state remove --force --yes --stack {args.stack}`.'
        )
        return 0

    if not azure_urns:
        print('\nNothing to remove.')
        return 0

    ordered = leaves_first_order(azure_urns, resources)
    print(f'\nRemoving {len(ordered)} URN(s) leaves-first...')
    succeeded: list[str] = []
    failed: list[tuple[str, int]] = []
    for urn in ordered:
        try:
            pulumi_state_remove_one(args.stack, urn, args.pulumi_dir)
            succeeded.append(urn)
        except subprocess.CalledProcessError as err:
            failed.append((urn, err.returncode))
            print(
                f'  FAILED (exit {err.returncode}): {urn}',
                file=sys.stderr,
            )
    print(f'\nDone. {len(succeeded)} removed, {len(failed)} failed.')
    if failed:
        print('Failed URNs:', file=sys.stderr)
        for urn, rc in failed:
            print(f'  (exit {rc}) {urn}', file=sys.stderr)
    if succeeded:
        print('Run the manual-command block above.')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
