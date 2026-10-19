"""Tests for the Themis roster and the access the driver grants Themis."""

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
from cpg_infra.driver.themis import build_roster, serialise_roster, themis_datasets

# Deliberately fictitious; the real accounts live in cpg-infrastructure-private.
AUTH_SA = 'not-a-real-auth@nonexistent-project.iam.gserviceaccount.com'
DATA_SA = 'not-a-real-data@nonexistent-project.iam.gserviceaccount.com'

USERS = {
    key: CPGInfrastructureUser.model_validate(
        {'id': key, 'clouds': {'gcp': {'id': gcp_id}} if gcp_id else {}},
    )
    for key, gcp_id in {
        'alice': 'Alice.Smith@Example.org',
        'bob': 'bob@example.com',
        'carol': 'carol@example.com',
        'no-gcp': None,
        'not-an-email': 'some-id-without-an-at-sign',
    }.items()
}


DEFAULT_PROJECTS = {
    'p1': {
        'name': 'Project one',
        'datasets': ['ds-b', 'ds-a'],
        'members': ['bob', 'alice'],
    },
}


def _themis(**overrides: Any) -> dict[str, Any]:
    return {
        'roster_bucket_suffix': 'themis-roster',
        'auth_service_account': AUTH_SA,
        'data_service_account': DATA_SA,
        'operators': ['carol'],
        **overrides,
    }


def _config(
    themis: dict[str, Any] | None = None,
    projects: dict[str, dict[str, Any]] | None = None,
) -> CPGInfrastructureConfig:
    """An infrastructure config with a Themis section and the given Projects."""
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
            'themis': _themis() if themis is None else themis,
            'themis_projects': DEFAULT_PROJECTS if projects is None else projects,
        },
    )


def _roster(config: CPGInfrastructureConfig, datasets: Any = None) -> dict[str, Any]:
    assert config.themis
    return build_roster(
        config.themis,
        config.themis_projects,
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


# region config


def test_without_themis_nothing_is_configured():
    config = CPGInfrastructureConfig.model_validate(
        _config().model_dump(exclude={'themis', 'themis_projects'}),
    )
    assert config.themis is None
    assert config.themis_projects == {}


@pytest.mark.parametrize(
    ('themis', 'projects', 'message'),
    [
        (None, DEFAULT_PROJECTS, 'no themis section'),
        (_themis(), {}, 'no Themis Project'),
    ],
)
def test_themis_section_and_enabled_projects_come_together(
    themis: dict[str, Any] | None,
    projects: dict[str, Any],
    message: str,
):
    payload = _config().model_dump()
    payload['themis'] = themis
    payload['themis_projects'] = projects
    with pytest.raises(ValidationError, match=message):
        CPGInfrastructureConfig.model_validate(payload)


@pytest.mark.parametrize(
    'project_id',
    ['neuro/ataxia', 'Upper-case', '-leading-hyphen', 'has space', ''],
)
def test_project_id_must_be_a_slug(project_id: str):
    with pytest.raises(ValidationError):
        _config(projects={project_id: DEFAULT_PROJECTS['p1']})


@pytest.mark.parametrize(
    'project',
    [
        {'name': 'P', 'datasets': [], 'members': []},
        {'name': '', 'datasets': ['ds-a'], 'members': []},
        {'name': 'P', 'datasets': ['ds-a', 'ds-a'], 'members': []},
        {'name': 'P', 'datasets': ['ds-a'], 'members': ['bob', 'bob']},
        {'name': 'P', 'datasets': ['ds-a']},
        {'name': 'P', 'datasets': ['ds-a'], 'members': [], 'unknown': 1},
    ],
)
def test_invalid_project_is_rejected(project: dict[str, Any]):
    with pytest.raises(ValidationError):
        _config(projects={'p1': project})


@pytest.mark.parametrize(
    'overrides',
    [
        {'auth_service_account': 'someone@example.com'},
        {'data_service_account': 'themis-data@project.iam.gserviceaccount.com.evil'},
        {'operators': ['carol', 'carol']},
        # Projects come from themis_projects, not the section
        {'projects': DEFAULT_PROJECTS},
    ],
)
def test_invalid_themis_section_is_rejected(overrides: dict[str, Any]):
    with pytest.raises(ValidationError):
        _config(themis=_themis(**overrides))


# endregion
# region roster


def test_roster_resolves_keys_to_lowercase_emails():
    assert _roster(_config()) == {
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
        themis=_themis(operators=['carol', 'alice']),
        projects={
            'p1': {'name': 'P1', 'datasets': ['ds-a'], 'members': ['alice']},
            'p2': {'name': 'P2', 'datasets': ['ds-b'], 'members': ['bob']},
            'p3': {'name': 'P3', 'datasets': ['ds-c'], 'members': []},
        },
    )
    roster = _roster(config)

    assert set(roster['projects']) == {'p1', 'p2', 'p3'}
    members = {m for p in roster['projects'].values() for m in p['members']}
    assert set(roster['admitted']) == members | {
        'carol@example.com',
        'alice.smith@example.org',
    }
    assert roster['admitted'] == sorted(set(roster['admitted']))
    assert roster['projects']['p3']['members'] == []


def test_serialised_roster_is_deterministic():
    """Config order does not reach the output, so an unchanged config writes no diff."""
    forward = _config(
        themis=_themis(operators=['carol', 'bob']),
        projects={
            'p1': {'name': 'P1', 'datasets': ['ds-a', 'ds-b'], 'members': ['alice']},
            'p2': {'name': 'P2', 'datasets': ['ds-c'], 'members': ['bob', 'carol']},
        },
    )
    reverse = _config(
        themis=_themis(operators=['bob', 'carol']),
        projects={
            'p2': {'name': 'P2', 'datasets': ['ds-c'], 'members': ['carol', 'bob']},
            'p1': {'name': 'P1', 'datasets': ['ds-b', 'ds-a'], 'members': ['alice']},
        },
    )

    serialised = serialise_roster(_roster(forward))
    assert serialised == serialise_roster(_roster(reverse))
    assert serialised.endswith('}\n')
    assert json.loads(serialised) == _roster(forward)


@pytest.mark.parametrize(
    ('config', 'message'),
    [
        (
            _config(
                projects={
                    'p1': {'name': 'P', 'datasets': ['ds-a'], 'members': ['dave']},
                },
            ),
            "project 'p1': member 'dave' not found",
        ),
        (_config(themis=_themis(operators=['dave'])), "operators: member 'dave'"),
        (_config(themis=_themis(operators=['no-gcp'])), "'no-gcp' does not have"),
        (_config(themis=_themis(operators=['not-an-email'])), 'not an email'),
    ],
)
def test_unresolvable_member_raises(config: CPGInfrastructureConfig, message: str):
    with pytest.raises(ValueError, match=message):
        _roster(config)


def test_unconfigured_dataset_raises():
    datasets = {'ds-a': DATASETS['ds-a']}
    message = "dataset 'ds-b', which is not configured"
    with pytest.raises(ValueError, match=message):
        _roster(_config(), datasets)
    with pytest.raises(ValueError, match=message):
        themis_datasets(_config().themis_projects, datasets)


def test_themis_datasets_is_the_sorted_union_of_project_datasets():
    config = _config(
        projects={
            'p1': {'name': 'P1', 'datasets': ['ds-b', 'ds-a'], 'members': []},
            'p2': {'name': 'P2', 'datasets': ['ds-a'], 'members': []},
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
    # common_gcp_infra is a cached_property; seed the cache with the mock
    root.__dict__['common_gcp_infra'] = common_gcp_infra

    cloud_infras = {d: MagicMock(spec=CPGDatasetCloudInfrastructure) for d in DATASETS}
    root.dataset_infrastructures = {
        d: MagicMock(clouds={'gcp': cloud_infra})
        for d, cloud_infra in cloud_infras.items()
    }
    return root, common_gcp_infra, cloud_infras


def test_setup_themis_writes_the_roster_and_grants_access():
    config = _config()
    root, common_gcp_infra, cloud_infras = _make_root(config)
    root.setup_themis()

    common_gcp_infra.create_bucket.assert_called_once()
    bucket_call = common_gcp_infra.create_bucket.call_args
    assert bucket_call.args == ('cpg-themis-roster',)
    assert bucket_call.kwargs['unique']
    assert bucket_call.kwargs['versioning']
    assert bucket_call.kwargs['enforce_public_access_prevention']
    bucket = common_gcp_infra.create_bucket.return_value

    blob = common_gcp_infra.add_blob_to_bucket.call_args.kwargs
    assert blob['bucket'] is bucket
    assert blob['output_name'] == 'roster.json'
    assert blob['contents'] == serialise_roster(_roster(config))

    common_gcp_infra.add_member_to_bucket.assert_called_once()
    grant = common_gcp_infra.add_member_to_bucket.call_args.kwargs
    assert grant['bucket'] is bucket
    assert grant['member'] == AUTH_SA
    assert grant['membership'] == BucketMembership.READ

    # ds-c is configured, but no Themis project lists it
    for dataset, cloud_infra in cloud_infras.items():
        reader = cast('MagicMock', cloud_infra.add_themis_data_reader)
        if dataset == 'ds-c':
            reader.assert_not_called()
        else:
            reader.assert_called_once_with(DATA_SA)


def test_setup_themis_is_a_no_op_without_themis_config():
    config = CPGInfrastructureConfig.model_validate(
        _config().model_dump(exclude={'themis', 'themis_projects'}),
    )
    root, common_gcp_infra, cloud_infras = _make_root(config)
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
    """A partial deploy would publish a roster whose data grants never landed."""
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
    driver.add_themis_data_reader(DATA_SA)

    groups = driver.group_provider.groups['gcp']
    joined = {name for name, g in groups.items() if DATA_SA in _direct_members(g)}
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

    driver.add_themis_data_reader(DATA_SA)
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
    assert any(DATA_SA in resolved(g) for g in main_bucket_readers)
    # update_metamist_members sends this group's members as the project's readers
    assert DATA_SA in resolved(driver.metamist_groups[SM_MAIN_READ])
    assert DATA_SA in resolved(invokers)


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
        driver.add_themis_data_reader(DATA_SA)


# endregion
