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
            f"--stack {stack} {shlex.quote(urn)}"
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
    """Return the parsed output of `pulumi stack export --stack <stack>`."""
    result = subprocess.run(  # noqa: S603
        ['pulumi', 'stack', 'export', '--stack', stack],
        cwd=pulumi_dir,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def pulumi_state_remove(
    stack: str, urns: list[str], pulumi_dir: str | None
) -> None:
    """One `pulumi state remove --force --yes --stack <stack> <urn>...` call.

    --force lets Pulumi proceed even when non-Azure survivors still hold
    references to the removed URNs (those refs are what the manual-command
    block below is for). --yes skips the interactive prompt.

    Raises CalledProcessError on non-zero exit -- the caller reports it.
    """
    cmd = [
        'pulumi', 'state', 'remove',
        '--force', '--yes',
        '--stack', stack,
        *urns,
    ]
    subprocess.run(cmd, cwd=pulumi_dir, check=True)  # noqa: S603


def main() -> int:
    args = parse_args()
    state = load_state(args.stack, args.pulumi_dir)
    resources = state['deployment']['resources']
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

    print(f'\nRemoving {len(azure_urns)} URN(s)...')
    try:
        pulumi_state_remove(args.stack, sorted(azure_urns), args.pulumi_dir)
    except subprocess.CalledProcessError as err:
        print(f'ERROR: pulumi state remove exited {err.returncode}', file=sys.stderr)
        return err.returncode
    print('Done. Run the manual-command block above.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
