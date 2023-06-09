import json
import graphlib

def main(state_file: str):
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
        state['checkpoint']['latest']['resources'] = [resource_by_urn[urn] for urn in graphlib.TopologicalSorter(deps).static_order()]
        with open(new_file_name, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)

if __name__ == "__main__":
    main("/Users/mfranklin/source/cpg-infrastructure-private/production-2023-06-09-group-import-test-reordered.json")