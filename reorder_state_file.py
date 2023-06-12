"""
Sometimes during tricky pulumi imports, the state file can get out of order.
This script will check for that and re-order the state file if necessary.

DANGER:
    This operates on state directly, you should very carefully check the output
    and keep backups before overwriting the state file.
"""
import json
import graphlib
import click


def main(state_file: str):
    """
    Check that the dependencies in the state file are listed in the correct order,
    and re-order if so. This can sometimes happen on renames
    """
    with open(state_file, encoding='utf-8') as f:
        state = json.load(f)

    deps = {}
    resources = state['checkpoint']['latest']['resources']
    resource_by_urn = {resource['urn']: resource for resource in resources}
    has_ordering_issues = False
    for resource in resources:
        urn = resource['urn']
        resource_name = urn.split('::')[3]
        resource_deps = resource.get('dependencies', [])
        deps[urn] = resource_deps
        for dependency in resource_deps:
            if dependency not in deps:
                dep_name = dependency.split('::')[3]
                if dependency in resource_by_urn:
                    has_ordering_issues = True
                    print(f'{resource_name} : Dep not listed yet: {dep_name}')
                else:
                    raise ValueError(f'{resource_name} : Dep not found: {dep_name}')

    if has_ordering_issues:
        new_file_name = state_file.replace('.json', '-reordered.json')
        new_resources = [
            resource_by_urn[urn]
            for urn in graphlib.TopologicalSorter(deps).static_order()
        ]
        print(f'Writing reordered state file to {new_file_name}')
        state['checkpoint']['latest']['resources'] = new_resources
        with open(new_file_name, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4)


@click.command()
@click.argument('state_file')
def from_cli(state_file: str):
    """Run from CLI args"""
    main(state_file)


if __name__ == '__main__':
    from_cli()  # pylint: disable=no-value-for-parameter
