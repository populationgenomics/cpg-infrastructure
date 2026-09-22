import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))


def test_module_imports_without_third_party_deps():
    mod = importlib.import_module('remove_azure_resources')
    # Guard: the simplified script must not depend on google.cloud.storage.
    assert 'google.cloud.storage' not in sys.modules
    # Public surface expected in later tasks.
    assert hasattr(mod, 'load_state')
    assert hasattr(mod, 'parse_args')


import remove_azure_resources as rar  # noqa: E402


def test_is_azure_by_type_prefix():
    assert rar.is_azure_resource({'type': 'azure-native:storage:StorageAccount'})
    assert rar.is_azure_resource({'type': 'azuread:index/user:User'})


def test_is_not_azure_for_gcp_bucket_named_azure():
    assert not rar.is_azure_resource(
        {
            'type': 'gcp:storage/bucket:Bucket',
            'urn': 'urn:pulumi:prod::infra::gcp:storage/bucket:Bucket::dataset-azure-archive',
        }
    )


def test_is_not_azure_for_component_by_urn_name_alone():
    # Classifier is type-only: a bare `pulumi:pulumi:Component` whose URN
    # name contains `-azure-` is NOT classified as Azure. Real Azure
    # components self-identify via type (`:azure:` namespace or
    # `azure-native:` / `azuread:` prefix); the URN name is not consulted.
    assert not rar.is_azure_resource(
        {
            'type': 'pulumi:pulumi:Component',
            'urn': 'urn:pulumi:prod::infra::pulumi:pulumi:Component::dataset-azure-storage',
        }
    )


def test_is_not_azure_for_unknown_typed_component_named_azure():
    # Sanity: an unrelated type whose name contains `Azure` is not Azure.
    assert not rar.is_azure_resource(
        {
            'type': 'cpg_infra:reports:AzureCostAudit',
            'urn': 'urn:pulumi:prod::infra::cpg_infra:reports:AzureCostAudit::dataset-azure-cost',
        }
    )


def test_is_azure_for_namespaced_component_type():
    assert rar.is_azure_resource({'type': 'cpg_infra:azure:AzureInfra'})


def test_finds_parent_dependency_and_provider_refs():
    azure_urn = 'urn:pulumi:prod::infra::azure-native:storage:StorageAccount::acct'
    non_azure: list[dict] = [
        {
            'urn': 'urn:pulumi:prod::infra::gcp:x:Y::a',
            'parent': azure_urn,
        },
        {
            'urn': 'urn:pulumi:prod::infra::gcp:x:Y::b',
            'dependencies': [azure_urn, 'urn:pulumi:prod::infra::gcp:x:Y::unrelated'],
        },
        {
            'urn': 'urn:pulumi:prod::infra::gcp:x:Y::c',
            'provider': f'{azure_urn}::abcd-1234',
        },
        {
            'urn': 'urn:pulumi:prod::infra::gcp:x:Y::d',
            'propertyDependencies': {'endpoint': [azure_urn]},
        },
    ]
    problems = rar.find_cross_cloud_refs({azure_urn}, non_azure)
    kinds = sorted({kind for _, kind, _ in problems})
    assert kinds == ['dependency', 'parent', 'propertyDependency[endpoint]', 'provider']


def test_manual_commands_one_per_surviving_urn_with_context_comment():
    azure_urn = 'urn:pulumi:prod::infra::azure-native:storage:StorageAccount::acct'
    survivor = 'urn:pulumi:prod::infra::gcp:x:Y::a'
    problems = [
        (survivor, 'parent', azure_urn),
        (survivor, 'dependency', azure_urn),  # multiple edges collapse
    ]
    lines = rar.build_manual_commands(problems)
    # Report the survivor and its fields without generating a destructive command.
    assert len(lines) == 1
    assert lines[0].startswith('# ') and survivor in lines[0]
    assert 'parent' in lines[0] and 'dependency' in lines[0]


def test_manual_commands_empty_when_no_problems():
    assert rar.build_manual_commands([]) == []


def test_leaves_first_order_uses_dependency_edges():
    provider = 'urn:pulumi:prod::infra::pulumi:providers:azure-native::default'
    resource = 'urn:pulumi:prod::infra::azure-native:x:Y::resource'
    resources = [
        {'urn': provider, 'type': 'pulumi:providers:azure-native'},
        {'urn': resource, 'type': 'azure-native:x:Y', 'provider': f'{provider}::id'},
    ]

    assert rar.leaves_first_order({provider, resource}, resources) == [
        resource,
        provider,
    ]


def test_leaves_first_order_rejects_cycles():
    resources = [
        {'urn': 'urn:a', 'parent': 'urn:b'},
        {'urn': 'urn:b', 'parent': 'urn:a'},
    ]

    with pytest.raises(ValueError, match='cyclic'):
        rar.leaves_first_order({'urn:a', 'urn:b'}, resources)


def test_apply_reports_process_start_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    error = FileNotFoundError(2, 'No such file or directory')

    def fail_remove(_stack: str, _urn: str, _pulumi_dir: str | None) -> None:
        raise error

    monkeypatch.setattr(rar, 'pulumi_state_remove_one', fail_remove)

    rc = rar._apply_removals(  # noqa: SLF001
        {'urn:a'}, [{'urn': 'urn:a'}], 'stack', None, False
    )
    captured = capsys.readouterr()

    assert rc == 1
    assert 'Done. 0 removed, 1 failed.' in captured.out
    assert 'FileNotFoundError' in captured.err


def test_main_dry_run_prints_urns_and_commands(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    azure_urn = 'urn:pulumi:prod::infra::azure-native:storage:StorageAccount::acct'
    survivor_urn = 'urn:pulumi:prod::infra::gcp:x:Y::a'
    fake_state = {
        'deployment': {
            'resources': [
                {'type': 'azure-native:storage:StorageAccount', 'urn': azure_urn},
                {'type': 'gcp:x:Y', 'urn': survivor_urn, 'parent': azure_urn},
            ],
        },
    }
    monkeypatch.setattr(rar, 'load_state', lambda _stack, _pulumi_dir: fake_state)
    monkeypatch.setattr(sys, 'argv', ['remove_azure_resources.py'])

    rc = rar.main()
    out = capsys.readouterr().out
    assert rc == 0
    assert azure_urn in out
    assert survivor_urn in out
    assert 'No automatic delete command is provided' in out
    assert 'pulumi state delete --target-dependents' in out
    assert '--apply' in out  # dry-run banner mentions how to actually apply
