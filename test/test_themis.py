"""Tests for the Themis ACL files and the access the driver grants Themis."""

from __future__ import annotations

import json
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from cpg_infra.abstraction.base import BucketMembership
from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.config import (
    CPGDatasetConfig,
    CPGInfrastructureConfig,
    CPGInfrastructureUser,
)
from cpg_infra.driver.constants import SM_MAIN_READ
from cpg_infra.driver.dataset_cloud_infrastructure import (
    CPGDatasetCloudInfrastructure,
)
from cpg_infra.driver.groups import GroupMember, GroupProvider
from cpg_infra.driver.infrastructure import CPGInfrastructure
from cpg_infra.driver.themis import (
    build_acls,
    environment_projects,
    serialise_acls,
    themis_datasets,
)


# Deliberately fictitious; the real accounts live in cpg-infrastructure-private.
def _sa(name: str) -> str:
    return f'not-a-real-{name}@nonexistent-project.iam.gserviceaccount.com'


USERS = {
    key: CPGInfrastructureUser.model_validate(
        {'id': key, 'clouds': {'gcp': {'id': gcp_id}} if gcp_id else {}},
    )
    for key, gcp_id in {
        'alice': 'Alice.Smith@Example.org',
        'bob': 'bob@example.com',
        'carol': 'carol@example.com',
        'dave': 'dave@example.com',
        'no-gcp': None,
        'not-an-email': 'some-id-without-an-at-sign',
    }.items()
}

DEFAULT_PROJECTS = {
    'p1': {
        'name': 'Project one',
        'datasets': ['ds-b', 'ds-a'],
        'members': ['bob', 'alice'],
        'environments': ['dev', 'prod'],
    },
}


def _settings(environment: str, **overrides: Any) -> dict[str, Any]:
    return {
        'auth_service_account': _sa(f'auth-{environment}'),
        'data_service_account': _sa(f'data-{environment}'),
        'operators': ['carol'],
        **overrides,
    }


def _config(
    themis: dict[str, Any] | None = None,
    projects: dict[str, dict[str, Any]] | None = None,
) -> CPGInfrastructureConfig:
    """An infrastructure config with the given Themis environments and Projects."""
    return CPGInfrastructureConfig.model_validate(
        {
            'domain': 'example.org',
            'budget_currency': 'AUD',
            'common_dataset': 'common',
            'config_destination': 'gs://nonexistent-config/',
            'users': USERS,
            'plugins_enabled': [],
            'gcp': {
                'customer_id': 'C0',
                'region': 'australia-southeast1',
                'groups_domain': 'example.org',
                'budget_notification_pubsub': None,
                'config_bucket_name': 'nonexistent-config',
                'dataset_storage_prefix': 'cpg-',
            },
            'themis': (
                {e: _settings(e) for e in ('dev', 'prod')} if themis is None else themis
            ),
            'themis_projects': DEFAULT_PROJECTS if projects is None else projects,
        },
    )


def _acls(
    config: CPGInfrastructureConfig,
    environment: str = 'prod',
    datasets: Any = None,
) -> dict[str, Any]:
    return build_acls(
        config.themis[environment],
        environment_projects(config.themis_projects, environment),
        config.users,
        DATASETS if datasets is None else datasets,
    )


def _dataset(name: str, **extra: Any) -> CPGDatasetConfig:
    return CPGDatasetConfig.model_validate(
        {
            'dataset': name,
            'budgets': {},
            'gcp': {'project': f'{name}-1234'},
            **extra,
        },
    )


DATASETS = {d: _dataset(d) for d in ('ds-a', 'ds-b', 'ds-c')}


def _project(
    datasets: list[str],
    members: list[str],
    environments: list[str],
) -> dict[str, Any]:
    return {
        'name': 'P',
        'datasets': datasets,
        'members': members,
        'environments': environments,
    }


# region config


def test_without_themis_nothing_is_configured():
    config = _config(themis={}, projects={})
    assert config.themis == {}
    assert config.themis_projects == {}


def test_project_in_an_environment_without_settings_is_rejected():
    with pytest.raises(ValidationError, match=r"names environments \['prod'\]"):
        _config(themis={'dev': _settings('dev')})


@pytest.mark.parametrize(
    'project_id',
    ['neuro/ataxia', 'Upper-case', '-leading-hyphen', 'has space', ''],
)
def test_project_id_must_be_a_slug(project_id: str):
    with pytest.raises(ValidationError, match='string_pattern_mismatch'):
        _config(projects={project_id: DEFAULT_PROJECTS['p1']})


@pytest.mark.parametrize('environment', ['Prod', 'dev/x', '-dev', ''])
def test_environment_must_be_a_slug(environment: str):
    with pytest.raises(ValidationError, match='string_pattern_mismatch'):
        _config(themis={environment: _settings('x')}, projects={})


@pytest.mark.parametrize(
    ('project', 'message'),
    [
        ({**DEFAULT_PROJECTS['p1'], 'datasets': []}, 'too_short'),
        ({**DEFAULT_PROJECTS['p1'], 'environments': []}, 'too_short'),
        ({**DEFAULT_PROJECTS['p1'], 'name': ''}, 'too_short'),
        ({**DEFAULT_PROJECTS['p1'], 'datasets': ['ds-a', 'ds-a']}, 'duplicate'),
        ({**DEFAULT_PROJECTS['p1'], 'members': ['bob', 'bob']}, 'duplicate'),
        ({**DEFAULT_PROJECTS['p1'], 'environments': ['dev', 'dev']}, 'duplicate'),
        ({**DEFAULT_PROJECTS['p1'], 'unknown': 1}, 'extra_forbidden'),
        (
            {k: v for k, v in DEFAULT_PROJECTS['p1'].items() if k != 'members'},
            'missing',
        ),
    ],
)
def test_invalid_project_is_rejected(project: dict[str, Any], message: str):
    with pytest.raises(ValidationError, match=message):
        _config(projects={'p1': project})


@pytest.mark.parametrize(
    ('overrides', 'message'),
    [
        ({'auth_service_account': 'someone@example.com'}, 'string_pattern'),
        (
            {'data_service_account': 'data@project.iam.gserviceaccount.com.evil'},
            'string_pattern',
        ),
        ({'operators': ['carol', 'carol']}, 'duplicate'),
        ({'bucket_suffix': 'themis-acls'}, 'extra_forbidden'),
    ],
)
def test_invalid_environment_settings_are_rejected(
    overrides: dict[str, Any],
    message: str,
):
    with pytest.raises(ValidationError, match=message):
        _config(
            themis={'dev': _settings('dev'), 'prod': _settings('prod', **overrides)}
        )


def test_environments_may_not_share_a_data_service_account():
    shared = {'data_service_account': _sa('data')}
    with pytest.raises(ValidationError, match='share a data service account'):
        _config(
            themis={
                'dev': _settings('dev', **shared),
                'prod': _settings('prod', **shared),
            },
        )


# endregion
# region ACLs


def test_acls_resolve_keys_to_lowercase_emails():
    assert _acls(_config()) == {
        'format': 1,
        'admitted': [
            'alice.smith@example.org',
            'bob@example.com',
            'carol@example.com',
        ],
        'projects': {
            'p1': {
                'name': 'Project one',
                'datasets': ['ds-a', 'ds-b'],
                'members': ['alice.smith@example.org', 'bob@example.com'],
            },
        },
    }


def test_admitted_is_the_union_of_operators_and_every_projects_members():
    config = _config(
        themis={
            'dev': _settings('dev'),
            'prod': _settings('prod', operators=['carol', 'alice']),
        },
        projects={
            'p1': _project(['ds-a'], ['alice'], ['prod']),
            'p2': _project(['ds-b'], ['bob'], ['prod']),
            'p3': _project(['ds-c'], [], ['prod']),
        },
    )
    acls = _acls(config)

    assert set(acls['projects']) == {'p1', 'p2', 'p3'}
    members = {m for p in acls['projects'].values() for m in p['members']}
    assert set(acls['admitted']) == members | {
        'carol@example.com',
        'alice.smith@example.org',
    }
    assert acls['admitted'] == sorted(set(acls['admitted']))
    assert acls['projects']['p3']['members'] == []


def test_environment_without_projects_admits_its_operators_alone():
    """Themis accepts an ACL file with no Projects."""
    config = _config(projects={'p1': _project(['ds-a'], ['bob'], ['prod'])})
    assert _acls(config, 'dev') == {
        'format': 1,
        'admitted': ['carol@example.com'],
        'projects': {},
    }


def test_serialised_acls_are_deterministic():
    """Config order does not reach the output, so an unchanged config writes no diff."""
    forward = _config(
        themis={'prod': _settings('prod', operators=['carol', 'bob'])},
        projects={
            'p1': _project(['ds-a', 'ds-b'], ['alice'], ['prod']),
            'p2': _project(['ds-c'], ['bob', 'carol'], ['prod']),
        },
    )
    reverse = _config(
        themis={'prod': _settings('prod', operators=['bob', 'carol'])},
        projects={
            'p2': _project(['ds-c'], ['carol', 'bob'], ['prod']),
            'p1': _project(['ds-b', 'ds-a'], ['alice'], ['prod']),
        },
    )

    serialised = serialise_acls(_acls(forward))
    assert serialised == serialise_acls(_acls(reverse))
    assert serialised.endswith('}\n')
    assert json.loads(serialised) == _acls(forward)


@pytest.mark.parametrize(
    ('config', 'message'),
    [
        (
            _config(projects={'p1': _project(['ds-a'], ['eve'], ['prod'])}),
            "project 'p1': member 'eve' not found",
        ),
        (
            _config(themis={'prod': _settings('prod', operators=['eve'])}, projects={}),
            "operators: member 'eve'",
        ),
        (
            _config(
                themis={'prod': _settings('prod', operators=['no-gcp'])}, projects={}
            ),
            "'no-gcp' does not have",
        ),
        (
            _config(
                themis={'prod': _settings('prod', operators=['not-an-email'])},
                projects={},
            ),
            'not an email',
        ),
    ],
)
def test_unresolvable_member_raises(config: CPGInfrastructureConfig, message: str):
    with pytest.raises(ValueError, match=message):
        _acls(config)


def test_unconfigured_dataset_raises():
    datasets = {'ds-a': DATASETS['ds-a']}
    message = "dataset 'ds-b', which is not configured"
    with pytest.raises(ValueError, match=message):
        _acls(_config(), datasets=datasets)
    with pytest.raises(ValueError, match=message):
        themis_datasets(_config().themis_projects, datasets)


def test_themis_datasets_is_the_sorted_union_of_project_datasets():
    config = _config(
        projects={
            'p1': _project(['ds-b', 'ds-a'], [], ['prod']),
            'p2': _project(['ds-a'], [], ['prod']),
        },
    )
    assert themis_datasets(config.themis_projects, DATASETS) == ['ds-a', 'ds-b']


# endregion
# region driver


def _make_root(
    config: CPGInfrastructureConfig,
) -> tuple[CPGInfrastructure, MagicMock, dict[str, MagicMock]]:
    """A CPGInfrastructure with only what setup_themis touches.

    Constructing a real one walks the whole plugin and group machinery.
    """
    root = CPGInfrastructure.__new__(CPGInfrastructure)
    root.config = config
    root.dataset_configs = DATASETS

    common_gcp_infra = MagicMock(spec=GcpInfrastructure)
    common_gcp_infra.create_bucket.side_effect = lambda name, **_: f'bucket:{name}'
    # common_gcp_infra is a cached_property; seed the cache with the mock
    root.__dict__['common_gcp_infra'] = common_gcp_infra

    cloud_infras = {d: MagicMock(spec=CPGDatasetCloudInfrastructure) for d in DATASETS}
    root.dataset_infrastructures = {
        d: MagicMock(clouds={'gcp': cloud_infra})
        for d, cloud_infra in cloud_infras.items()
    }
    return root, common_gcp_infra, cloud_infras


def _written(common_gcp_infra: MagicMock) -> dict[str, dict[str, Any]]:
    """Bucket name -> the ACL file written to it."""
    written = {}
    for call in common_gcp_infra.add_blob_to_bucket.call_args_list:
        assert call.kwargs['output_name'] == 'acls.json'
        bucket = call.kwargs['bucket'].removeprefix('bucket:')
        written[bucket] = json.loads(call.kwargs['contents'])
    return written


def _data_readers(cloud_infras: dict[str, MagicMock]) -> dict[str, set[str]]:
    """Dataset -> the service accounts given read on it."""
    return {
        dataset: {
            call.args[0]
            for call in cast(
                'MagicMock',
                cloud_infra.add_themis_data_reader,
            ).call_args_list
        }
        for dataset, cloud_infra in cloud_infras.items()
    }


def test_setup_themis_writes_each_environments_acls_and_grants_access():
    config = _config()
    root, common_gcp_infra, cloud_infras = _make_root(config)
    root.setup_themis()

    buckets = {
        call.args[0]: call.kwargs
        for call in common_gcp_infra.create_bucket.call_args_list
    }
    assert set(buckets) == {'cpg-themis-dev-acls', 'cpg-themis-prod-acls'}
    for kwargs in buckets.values():
        assert kwargs['unique']
        assert kwargs['versioning']
        assert kwargs['enforce_public_access_prevention']

    assert _written(common_gcp_infra) == {
        'cpg-themis-dev-acls': _acls(config, 'dev'),
        'cpg-themis-prod-acls': _acls(config, 'prod'),
    }

    grants = {
        (call.kwargs['bucket'], call.kwargs['member'], call.kwargs['membership'])
        for call in common_gcp_infra.add_member_to_bucket.call_args_list
    }
    assert grants == {
        ('bucket:cpg-themis-dev-acls', _sa('auth-dev'), BucketMembership.READ),
        ('bucket:cpg-themis-prod-acls', _sa('auth-prod'), BucketMembership.READ),
    }

    # ds-c is configured, but no Themis Project lists it
    both = {_sa('data-dev'), _sa('data-prod')}
    assert _data_readers(cloud_infras) == {'ds-a': both, 'ds-b': both, 'ds-c': set()}


def test_environments_are_separate():
    """A Project enabled in one environment reaches neither the other's ACL file
    nor the other's data service account."""
    config = _config(
        projects={
            'shared': _project(['ds-a'], ['alice'], ['dev', 'prod']),
            'dev-only': _project(['ds-c'], ['dave'], ['dev']),
            'prod-only': _project(['ds-b'], ['bob'], ['prod']),
        },
    )
    root, common_gcp_infra, cloud_infras = _make_root(config)
    root.setup_themis()

    written = _written(common_gcp_infra)
    dev, prod = written['cpg-themis-dev-acls'], written['cpg-themis-prod-acls']
    assert set(dev['projects']) == {'shared', 'dev-only'}
    assert set(prod['projects']) == {'shared', 'prod-only'}
    assert 'dave@example.com' in dev['admitted']
    assert 'dave@example.com' not in prod['admitted']
    assert 'bob@example.com' in prod['admitted']
    assert 'bob@example.com' not in dev['admitted']

    assert _data_readers(cloud_infras) == {
        'ds-a': {_sa('data-dev'), _sa('data-prod')},
        'ds-b': {_sa('data-prod')},
        'ds-c': {_sa('data-dev')},
    }


def test_setup_themis_is_a_no_op_without_themis_config():
    root, common_gcp_infra, cloud_infras = _make_root(_config(themis={}, projects={}))
    root.validate_themis()
    root.setup_themis()

    assert not common_gcp_infra.method_calls
    for cloud_infra in cloud_infras.values():
        assert not cloud_infra.method_calls


def test_dataset_without_a_gcp_deploy_location_raises():
    root, _, _ = _make_root(_config())
    root.dataset_infrastructures['ds-b'].clouds = {}
    with pytest.raises(ValueError, match="'ds-b', which has no 'gcp' deploy location"):
        root.validate_themis()


def test_unreadable_dataset_fails_before_any_resource_is_registered():
    """A partial deploy would publish ACLs whose data grants never landed."""
    root, common_gcp_infra, cloud_infras = _make_root(_config())
    cast(
        'MagicMock', cloud_infras['ds-b'].check_themis_data_readable
    ).side_effect = ValueError('ds-b: Themis reads')

    for step in (root.validate_themis, root.setup_themis):
        with pytest.raises(ValueError, match='ds-b: Themis reads'):
            step()
    assert not common_gcp_infra.method_calls
    for cloud_infra in cloud_infras.values():
        cast('MagicMock', cloud_infra.add_themis_data_reader).assert_not_called()


def _dataset_cloud_infra(
    dataset_config: CPGDatasetConfig,
) -> CPGDatasetCloudInfrastructure:
    infra = MagicMock(spec=GcpInfrastructure)
    infra.name.return_value = 'gcp'
    infra.get_pulumi_name.side_effect = lambda key: f'{dataset_config.dataset}-{key}'
    config = MagicMock(spec=CPGInfrastructureConfig)
    config.igv_proxy = None
    return CPGDatasetCloudInfrastructure(
        config=config,
        root=MagicMock(),
        group_provider=GroupProvider(),
        infra=infra,
        dataset_config=dataset_config,
    )


def _direct_members(group: Any) -> set[str]:
    return {m.cloud_id for m in group.members.values() if isinstance(m, GroupMember)}


def test_data_reader_joins_the_main_bucket_and_main_metamist_read_groups():
    driver = _dataset_cloud_infra(
        _dataset('ds-a', components={'gcp': ['storage', 'metamist']}),
    )
    for environment in ('dev', 'prod'):
        driver.add_themis_data_reader(
            _sa(f'data-{environment}'), environment=environment
        )

    groups = driver.group_provider.groups['gcp']
    for environment in ('dev', 'prod'):
        sa = _sa(f'data-{environment}')
        joined = {name for name, g in groups.items() if sa in _direct_members(g)}
        assert joined == {
            'ds-a-external-repository-reader',
            f'ds-a-sample-metadata-{SM_MAIN_READ}',
        }


def test_data_reader_reads_the_main_bucket_and_may_invoke_metamist():
    driver = _dataset_cloud_infra(
        _dataset('ds-a', components={'gcp': ['storage', 'metamist']}),
    )
    provider = driver.group_provider
    invokers = provider.create_group(driver.infra, 'invokers', cache_members=False)
    cast('MagicMock', driver.root).gcp_metamist_invoker_group = invokers
    data_sa = _sa('data-prod')

    driver.add_themis_data_reader(data_sa, environment='prod')
    driver.setup_storage_main_bucket_permissions()
    driver.setup_metamist_cloudrun_permissions()

    def resolved(group: Any) -> set[str]:
        return {m.cloud_id for m in provider.resolve_group_members(group)}

    infra = cast('MagicMock', driver.infra)
    main_bucket_readers = [
        call.args[2]
        for call in infra.add_member_to_bucket.call_args_list
        if call.args[1] is driver.main_bucket
        and call.args[3] == BucketMembership.READ
        and hasattr(call.args[2], 'is_group')
    ]
    assert any(data_sa in resolved(g) for g in main_bucket_readers)
    # update_metamist_members sends this group's members as the project's readers
    assert data_sa in resolved(driver.metamist_groups[SM_MAIN_READ])
    assert data_sa in resolved(invokers)


@pytest.mark.parametrize(
    'extra',
    [
        {'components': {'gcp': ['metamist']}},
        {'components': {'gcp': ['storage']}},
        {
            'components': {'gcp': ['storage', 'metamist']},
            'enable_metamist_project': False,
        },
    ],
)
def test_data_reader_needs_storage_and_metamist(extra: dict[str, Any]):
    driver = _dataset_cloud_infra(_dataset('ds-a', **extra))
    with pytest.raises(ValueError, match='ds-a: Themis reads'):
        driver.add_themis_data_reader(_sa('data-prod'), environment='prod')


# endregion
