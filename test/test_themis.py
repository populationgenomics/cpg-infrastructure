"""Tests for the Themis ACL files and the access the driver grants Themis."""

from __future__ import annotations

import json
from typing import Any, cast
from unittest.mock import MagicMock

import pulumi_gcp as gcp
import pytest
from pydantic import ValidationError

from cpg_infra.abstraction.base import BucketMembership
from cpg_infra.abstraction.gcp import GcpInfrastructure
from cpg_infra.config import (
    CPGDatasetConfig,
    CPGInfrastructureConfig,
    CPGInfrastructureUser,
    ThemisNamespace,
)
from cpg_infra.driver.constants import SM_MAIN_READ, SM_TEST_READ
from cpg_infra.driver.dataset_cloud_infrastructure import (
    CPGDatasetCloudInfrastructure,
)
from cpg_infra.driver.groups import GroupMember, GroupProvider
from cpg_infra.driver.infrastructure import CPGInfrastructure
from cpg_infra.driver.themis import build_acls, serialise_acls


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
        'non-ascii': 'zoë@example.com',
        'robot': 'Robot@some-project.iam.gserviceaccount.com',
        'compute': '1234-compute@developer.gserviceaccount.com',
    }.items()
}


def _dataset(name: str, **extra: Any) -> CPGDatasetConfig:
    return CPGDatasetConfig.model_validate(
        {
            'dataset': name,
            'budgets': {},
            'gcp': {'project': f'{name}-1234'},
            **extra,
        },
    )


DATASETS = {
    'ds-a': _dataset(
        'ds-a',
        members={
            'themis-manager': ['bob', 'alice'],
            'themis-curator': ['alice'],
            'web-access': ['dave'],
        },
    ),
    # curators only
    'ds-b': _dataset('ds-b', members={'themis-curator': ['carol', 'bob']}),
    # no Themis role list, so never a Project
    'ds-c': _dataset('ds-c', members={'web-access': ['dave']}),
    # empty Themis role lists, so never a Project
    'ds-d': _dataset(
        'ds-d',
        members={'themis-manager': [], 'themis-curator': []},
    ),
}
ALL_DATASETS = sorted(DATASETS)


def _settings(environment: str, **overrides: Any) -> dict[str, Any]:
    return {
        'auth_service_account': _sa(f'auth-{environment}'),
        'data_service_account': _sa(f'data-{environment}'),
        'namespace': 'test' if environment == 'dev' else 'main',
        'allowed_datasets': ALL_DATASETS,
        **overrides,
    }


def _config(themis: dict[str, Any] | None = None) -> CPGInfrastructureConfig:
    """An infrastructure config with the given Themis environments."""
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
        },
    )


def _acls(
    allowed_datasets: list[str],
    datasets: dict[str, CPGDatasetConfig] | None = None,
) -> dict[str, Any]:
    return build_acls(
        allowed_datasets,
        DATASETS if datasets is None else datasets,
        USERS,
    )


# region config


def test_without_themis_nothing_is_configured():
    assert _config(themis={}).themis == {}


@pytest.mark.parametrize('environment', ['Prod', 'dev/x', '-dev', ''])
def test_environment_must_be_a_slug(environment: str):
    with pytest.raises(ValidationError, match='string_pattern_mismatch'):
        _config(themis={environment: _settings('x')})


@pytest.mark.parametrize(
    ('overrides', 'message'),
    [
        ({'auth_service_account': 'someone@example.com'}, 'string_pattern'),
        (
            {'data_service_account': 'data@project.iam.gserviceaccount.com.evil'},
            'string_pattern',
        ),
        ({'allowed_datasets': []}, 'too_short'),
        ({'namespace': 'main-upload'}, 'literal_error'),
        ({'namespace': 'Main'}, 'literal_error'),
        ({'allowed_datasets': ['ds-a', 'ds-a']}, 'duplicate'),
        # the site resolves its allowlist into allowed_datasets
        ({'project_allowlist': ['p1']}, 'extra_forbidden'),
        ({'operators': ['carol']}, 'extra_forbidden'),
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


@pytest.mark.parametrize('field', ['allowed_datasets', 'namespace'])
def test_settings_without_a_default_are_required(field: str):
    settings = _settings('prod')
    del settings[field]
    with pytest.raises(ValidationError, match='missing'):
        _config(themis={'prod': settings})


def test_environments_may_not_share_a_data_service_account():
    shared = {'data_service_account': _sa('data')}
    with pytest.raises(ValidationError, match='share a data service account'):
        _config(
            themis={
                'dev': _settings('dev', **shared),
                'prod': _settings('prod', **shared),
            },
        )


def test_datasets_may_list_themis_role_members():
    assert DATASETS['ds-a'].members['themis-manager'] == ['bob', 'alice']
    assert DATASETS['ds-a'].members['themis-curator'] == ['alice']


# endregion
# region ACLs


def test_acls_have_one_project_per_dataset_with_both_roles_as_lowercase_emails():
    assert _acls(['ds-a', 'ds-b']) == {
        'format': 1,
        'projects': {
            'ds-a': {
                'managers': ['alice.smith@example.org', 'bob@example.com'],
                'curators': ['alice.smith@example.org'],
            },
            'ds-b': {
                'managers': [],
                'curators': ['bob@example.com', 'carol@example.com'],
            },
        },
    }


@pytest.mark.parametrize(
    ('members', 'is_project'),
    [
        ({'themis-manager': ['bob']}, True),
        ({'themis-curator': ['bob']}, True),
        ({'themis-manager': [], 'themis-curator': []}, False),
        ({'web-access': ['bob']}, False),
    ],
)
def test_a_dataset_is_a_project_when_either_role_list_is_non_empty(
    members: dict[str, list[str]],
    is_project: bool,
):
    acls = _acls(['ds-x'], {'ds-x': _dataset('ds-x', members=members)})
    assert ('ds-x' in acls['projects']) is is_project


def test_only_allowed_datasets_are_projects():
    assert set(_acls(ALL_DATASETS)['projects']) == {'ds-a', 'ds-b'}
    assert set(_acls(['ds-b', 'ds-c'])['projects']) == {'ds-b'}


def test_acls_without_projects_have_an_empty_projects_map():
    """Themis accepts an ACL file with no Projects."""
    assert _acls(['ds-c', 'ds-d']) == {'format': 1, 'projects': {}}


def test_serialised_acls_are_deterministic():
    """Config order does not reach the output, so an unchanged config writes no diff."""
    reordered = {
        'ds-a': _dataset(
            'ds-a',
            members={'themis-curator': ['alice'], 'themis-manager': ['alice', 'bob']},
        ),
        'ds-b': _dataset('ds-b', members={'themis-curator': ['bob', 'carol']}),
    }
    serialised = serialise_acls(_acls(['ds-a', 'ds-b']))
    assert serialised == serialise_acls(_acls(['ds-b', 'ds-a'], reordered))
    assert serialised.endswith('}\n')
    assert json.loads(serialised) == _acls(['ds-a', 'ds-b'])


@pytest.mark.parametrize(
    ('member', 'message'),
    [
        ('eve', "member 'eve' not found"),
        ('no-gcp', "'no-gcp' has no Google account"),
        ('not-an-email', 'not the email of a Google account'),
        ('non-ascii', 'not the email of a Google account'),
        ('robot', "'robot' is the service account"),
        ('compute', "'compute' is the service account"),
    ],
)
@pytest.mark.parametrize('list_name', ['themis-manager', 'themis-curator'])
def test_unresolvable_member_raises(list_name: str, member: str, message: str):
    datasets = {'ds-x': _dataset('ds-x', members={list_name: [member]})}
    with pytest.raises(ValueError, match=f'ds-x {list_name}: .*{message}'):
        _acls(['ds-x'], datasets)


def test_unconfigured_allowed_dataset_raises():
    with pytest.raises(ValueError, match="dataset 'ds-z', which is not configured"):
        _acls(['ds-a', 'ds-z'])


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
    common_gcp_infra.region = 'australia-southeast1'
    # common_gcp_infra is a cached_property; seed the cache with the mock
    root.__dict__['common_gcp_infra'] = common_gcp_infra

    cloud_infras = {d: MagicMock(spec=CPGDatasetCloudInfrastructure) for d in DATASETS}
    root.dataset_infrastructures = {
        d: MagicMock(clouds={'gcp': cloud_infra})
        for d, cloud_infra in cloud_infras.items()
    }
    return root, common_gcp_infra, cloud_infras


@pytest.fixture
def created_buckets(monkeypatch: pytest.MonkeyPatch) -> dict[str, dict[str, Any]]:
    """Bucket name -> its arguments, for the buckets created without registering."""
    created: dict[str, dict[str, Any]] = {}

    def bucket(_resource_name: str, **kwargs: Any) -> str:
        created[kwargs['name']] = kwargs
        return f'bucket:{kwargs["name"]}'

    monkeypatch.setattr(gcp.storage, 'Bucket', bucket)
    return created


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


def test_setup_themis_writes_each_environments_acls_and_grants_access(
    created_buckets: dict[str, dict[str, Any]],
):
    root, common_gcp_infra, cloud_infras = _make_root(_config())
    root.setup_themis()

    assert set(created_buckets) == {'cpg-themis-dev-acls', 'cpg-themis-prod-acls'}
    for kwargs in created_buckets.values():
        assert kwargs['public_access_prevention'] == 'enforced'
        assert kwargs['uniform_bucket_level_access']
        assert kwargs['versioning'].enabled

    assert _written(common_gcp_infra) == {
        'cpg-themis-dev-acls': _acls(ALL_DATASETS),
        'cpg-themis-prod-acls': _acls(ALL_DATASETS),
    }

    grants = {
        (call.kwargs['bucket'], call.kwargs['member'], call.kwargs['membership'])
        for call in common_gcp_infra.add_member_to_bucket.call_args_list
    }
    assert grants == {
        ('bucket:cpg-themis-dev-acls', _sa('auth-dev'), BucketMembership.READ),
        ('bucket:cpg-themis-prod-acls', _sa('auth-prod'), BucketMembership.READ),
    }

    # ds-c and ds-d are allowed, but no Project is built from them
    both = {_sa('data-dev'), _sa('data-prod')}
    assert _data_readers(cloud_infras) == {
        'ds-a': both,
        'ds-b': both,
        'ds-c': set(),
        'ds-d': set(),
    }
    # each environment's data service account reads its own namespace
    namespaces = {
        (call.args[0], call.kwargs['environment'], call.kwargs['namespace'])
        for cloud_infra in cloud_infras.values()
        for call in cast('MagicMock', cloud_infra.add_themis_data_reader).call_args_list
    }
    assert namespaces == {
        (_sa('data-dev'), 'dev', 'test'),
        (_sa('data-prod'), 'prod', 'main'),
    }


@pytest.mark.usefixtures('created_buckets')
def test_data_service_account_reads_exactly_the_acl_files_datasets():
    config = _config(
        themis={
            'dev': _settings('dev', allowed_datasets=['ds-a', 'ds-c']),
            'prod': _settings('prod', allowed_datasets=['ds-b', 'ds-d']),
        },
    )
    root, common_gcp_infra, cloud_infras = _make_root(config)
    root.setup_themis()

    readers = _data_readers(cloud_infras)
    for bucket, acls in _written(common_gcp_infra).items():
        environment = bucket.removeprefix('cpg-themis-').removesuffix('-acls')
        assert {
            dataset
            for dataset, sas in readers.items()
            if _sa(f'data-{environment}') in sas
        } == set(acls['projects'])


@pytest.mark.usefixtures('created_buckets')
def test_environments_are_separate():
    """A dataset allowed in one environment reaches neither the other's ACL file
    nor the other's data service account."""
    config = _config(
        themis={
            'dev': _settings('dev', allowed_datasets=['ds-a']),
            'prod': _settings('prod', allowed_datasets=['ds-b']),
        },
    )
    root, common_gcp_infra, cloud_infras = _make_root(config)
    root.setup_themis()

    written = _written(common_gcp_infra)
    dev, prod = written['cpg-themis-dev-acls'], written['cpg-themis-prod-acls']
    assert dev['projects'] == {
        'ds-a': {
            'managers': ['alice.smith@example.org', 'bob@example.com'],
            'curators': ['alice.smith@example.org'],
        },
    }
    assert prod['projects'] == {
        'ds-b': {
            'managers': [],
            'curators': ['bob@example.com', 'carol@example.com'],
        },
    }

    assert _data_readers(cloud_infras) == {
        'ds-a': {_sa('data-dev')},
        'ds-b': {_sa('data-prod')},
        'ds-c': set(),
        'ds-d': set(),
    }


def test_setup_themis_is_a_no_op_without_themis_config(
    created_buckets: dict[str, dict[str, Any]],
):
    root, common_gcp_infra, cloud_infras = _make_root(_config(themis={}))
    root.validate_themis()
    root.setup_themis()

    assert not created_buckets
    assert not common_gcp_infra.method_calls
    for cloud_infra in cloud_infras.values():
        assert not cloud_infra.method_calls


def test_bucket_name_over_the_gcs_limit_fails_validation(
    created_buckets: dict[str, dict[str, Any]],
):
    # 'cpg-themis-' + environment + '-acls' is 63 characters at the limit
    at_limit, over_limit = 'e' * 47, 'e' * 48
    root, _, _ = _make_root(_config(themis={at_limit: _settings('x')}))
    root.validate_themis()

    root, common_gcp_infra, _ = _make_root(_config(themis={over_limit: _settings('x')}))
    with pytest.raises(ValueError, match='longer than 63 characters'):
        root.validate_themis()
    with pytest.raises(ValueError, match='longer than 63 characters'):
        root.setup_themis()
    assert not created_buckets
    assert not common_gcp_infra.method_calls


def test_themis_without_gcp_config_fails_validation():
    root, _, _ = _make_root(_config().model_copy(update={'gcp': None}))
    with pytest.raises(ValueError, match='needs the gcp config'):
        root.validate_themis()


def test_dataset_without_a_gcp_deploy_location_raises():
    root, _, _ = _make_root(_config())
    root.dataset_infrastructures['ds-b'].clouds = {}
    with pytest.raises(ValueError, match="'ds-b', which has no 'gcp' deploy location"):
        root.validate_themis()


def test_unreadable_dataset_fails_before_any_resource_is_registered(
    created_buckets: dict[str, dict[str, Any]],
):
    """A partial deploy would publish ACLs whose data grants never landed."""
    root, common_gcp_infra, cloud_infras = _make_root(_config())
    cast(
        'MagicMock', cloud_infras['ds-b'].check_themis_data_readable
    ).side_effect = ValueError('ds-b: Themis reads')

    for step in (root.validate_themis, root.setup_themis):
        with pytest.raises(ValueError, match='ds-b: Themis reads'):
            step()
    assert not created_buckets
    assert not common_gcp_infra.method_calls
    for cloud_infra in cloud_infras.values():
        cast('MagicMock', cloud_infra.add_themis_data_reader).assert_not_called()


def test_allowed_dataset_outside_the_acl_file_needs_no_themis_prerequisites():
    """Only the datasets Themis reads must be readable by it."""
    root, _, cloud_infras = _make_root(_config())
    root.dataset_infrastructures['ds-c'].clouds = {}
    cast(
        'MagicMock', cloud_infras['ds-d'].check_themis_data_readable
    ).side_effect = ValueError('ds-d: Themis reads')
    root.validate_themis()


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


@pytest.mark.parametrize(
    ('namespace', 'groups'),
    [
        (
            'main',
            {'ds-a-external-repository-reader', f'ds-a-sample-metadata-{SM_MAIN_READ}'},
        ),
        ('test', {f'ds-a-sample-metadata-{SM_TEST_READ}'}),
    ],
)
def test_data_reader_joins_only_its_namespaces_read_groups(
    namespace: ThemisNamespace,
    groups: set[str],
):
    driver = _dataset_cloud_infra(
        _dataset('ds-a', components={'gcp': ['storage', 'metamist']}),
    )
    driver.add_themis_data_reader(_sa('data-x'), environment='x', namespace=namespace)

    provider_groups = driver.group_provider.groups['gcp']
    joined = {
        name
        for name, g in provider_groups.items()
        if _sa('data-x') in _direct_members(g)
    }
    assert joined == groups


def _bucket_readers(
    driver: CPGDatasetCloudInfrastructure,
    bucket: Any,
) -> set[str]:
    """The accounts with read on `bucket`, directly or through a group."""
    readers = set()
    infra = cast('MagicMock', driver.infra)
    for call in infra.add_member_to_bucket.call_args_list:
        args = dict(
            zip(
                ('resource_key', 'bucket', 'member', 'membership'),
                call.args,
                strict=False,
            )
        )
        args.update(call.kwargs)
        if args['bucket'] is not bucket or args['membership'] != BucketMembership.READ:
            continue
        member = args['member']
        if hasattr(member, 'is_group'):
            readers |= {
                m.cloud_id for m in driver.group_provider.resolve_group_members(member)
            }
        else:
            readers.add(member)
    return readers


@pytest.mark.parametrize(
    ('namespace', 'bucket_name', 'metamist_read'),
    [('main', 'main', SM_MAIN_READ), ('test', 'test', SM_TEST_READ)],
)
def test_data_reader_reads_its_namespaces_bucket_and_may_invoke_metamist(
    namespace: ThemisNamespace,
    bucket_name: str,
    metamist_read: str,
):
    driver = _dataset_cloud_infra(
        _dataset('ds-a', components={'gcp': ['storage', 'metamist']}),
    )
    # a distinct mock per bucket, so the bucket a grant targets can be told apart
    cast('MagicMock', driver.infra).create_bucket.side_effect = (
        lambda name, **_: MagicMock(name=f'bucket-{name}')
    )
    cast('MagicMock', driver.config).web_service = None
    provider = driver.group_provider
    invokers = provider.create_group(driver.infra, 'invokers', cache_members=False)
    cast('MagicMock', driver.root).gcp_metamist_invoker_group = invokers
    data_sa = _sa('data-x')

    driver.add_themis_data_reader(data_sa, environment='x', namespace=namespace)
    driver.setup_storage_main_bucket_permissions()
    driver.setup_storage_test_buckets_permissions()
    driver.setup_metamist_cloudrun_permissions()

    buckets = {
        'main': driver.main_bucket,
        'test': driver.test_bucket,
        'test-analysis': driver.test_analysis_bucket,
        'test-tmp': driver.test_tmp_bucket,
    }
    read = {
        name for name, b in buckets.items() if data_sa in _bucket_readers(driver, b)
    }
    assert read == {bucket_name}
    # update_metamist_members sends this group's members as the project's readers
    resolved = {
        kind: {m.cloud_id for m in provider.resolve_group_members(group)}
        for kind, group in driver.metamist_groups.items()
    }
    assert {kind for kind, members in resolved.items() if data_sa in members} == {
        metamist_read,
    }
    assert data_sa in {m.cloud_id for m in provider.resolve_group_members(invokers)}


@pytest.mark.parametrize(
    ('namespace', 'extra', 'missing'),
    [
        ('main', {'components': {'gcp': ['metamist']}}, 'the storage component'),
        ('main', {'components': {'gcp': ['storage']}}, 'the metamist component'),
        (
            'main',
            {
                'components': {'gcp': ['storage', 'metamist']},
                'enable_metamist_project': False,
            },
            'enable_metamist_project',
        ),
        (
            'test',
            {'components': {'gcp': ['storage', 'metamist']}, 'setup_test': False},
            'setup_test',
        ),
    ],
)
def test_data_reader_needs_its_namespaces_storage_and_metamist(
    namespace: ThemisNamespace,
    extra: dict[str, Any],
    missing: str,
):
    driver = _dataset_cloud_infra(_dataset('ds-a', **extra))
    with pytest.raises(
        ValueError, match=f'ds-a: Themis reads its {namespace} .*{missing}'
    ):
        driver.add_themis_data_reader(
            _sa('data-x'), environment='x', namespace=namespace
        )


def test_main_namespace_does_not_need_setup_test():
    driver = _dataset_cloud_infra(
        _dataset(
            'ds-a',
            components={'gcp': ['storage', 'metamist']},
            setup_test=False,
        ),
    )
    driver.check_themis_data_readable('main')


def test_themis_role_members_join_no_group():
    """Themis roles reach a dataset only through Themis, never a Google group."""
    driver = _dataset_cloud_infra(
        _dataset(
            'ds-a',
            components={'gcp': ['storage', 'metamist']},
            members={
                'themis-manager': ['alice'],
                'themis-curator': ['carol'],
                'web-access': ['bob'],
            },
        ),
    )
    cast('MagicMock', driver.config).users = USERS
    driver.setup_externally_specified_members()

    groups = driver.group_provider.groups['gcp']
    joined = {
        email: {name for name, g in groups.items() if email in _direct_members(g)}
        for email in ('Alice.Smith@Example.org', 'carol@example.com', 'bob@example.com')
    }
    assert joined == {
        'Alice.Smith@Example.org': set(),
        'carol@example.com': set(),
        'bob@example.com': {'ds-a-web-access'},
    }


# endregion
