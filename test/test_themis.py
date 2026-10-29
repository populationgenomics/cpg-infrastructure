"""Tests for Themis's ACL and the access the driver grants Themis."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import pulumi
import pytest
from pydantic import ValidationError

from cpg_infra.abstraction.base import BucketMembership
from cpg_infra.abstraction.context import InfraContext
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
from cpg_infra.driver.themis import build_acl, serialise_acl

if TYPE_CHECKING:
    from collections.abc import Callable


# Deliberately fictitious; the real accounts live in cpg-infrastructure-private.
def _sa(name: str) -> str:
    return f'not-a-real-{name}@nonexistent-project.iam.gserviceaccount.com'


def _acl_secret(environment: str) -> str:
    return f'projects/not-a-real-themis-{environment}/secrets/acl'


def _user(key: str, gcp_id: str | None) -> CPGInfrastructureUser:
    return CPGInfrastructureUser.model_validate(
        {'id': key, 'clouds': {'gcp': {'id': gcp_id}} if gcp_id else {}},
    )


USERS = {
    key: _user(key, gcp_id)
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
        'acl_secret': _acl_secret(environment),
        'sandbox_service_account': _sa(f'sandbox-{environment}'),
        'namespace': 'test' if environment == 'dev' else 'main',
        'allowed_datasets': ALL_DATASETS,
        **overrides,
    }


def _config(
    themis: dict[str, Any] | None = None,
    users: dict[str, CPGInfrastructureUser] | None = None,
) -> CPGInfrastructureConfig:
    """An infrastructure config with the given Themis environments."""
    return CPGInfrastructureConfig.model_validate(
        {
            'domain': 'example.org',
            'budget_currency': 'AUD',
            'common_dataset': 'common',
            'config_destination': 'gs://nonexistent-config/',
            'users': USERS if users is None else users,
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


def _acl(
    allowed_datasets: list[str],
    datasets: dict[str, CPGDatasetConfig] | None = None,
) -> dict[str, Any]:
    return build_acl(
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
    'acl_secret',
    [
        'acl',
        'not-a-real-themis-prod/acl',
        'projects/not-a-real-themis-prod/secrets/acl/versions/1',
        'projects/not-a-real-themis-prod/secrets/',
        'projects/not-a-real-themis-prod/secrets/acl.json',
        'projects/not-a-real-themis-prod/secrets/' + 'a' * 256,
        # project ids: 6 to 30 characters, lowercase, starting with a letter,
        # not ending with a hyphen
        'projects/Not-A-Real-Themis-Prod/secrets/acl',
        'projects/abcde/secrets/acl',
        'projects/' + 'a' * 31 + '/secrets/acl',
        'projects/1-themis-prod/secrets/acl',
        'projects/themis-prod-/secrets/acl',
        'projects/themis_prod/secrets/acl',
    ],
)
def test_acl_secret_must_be_a_secret_resource_name(acl_secret: str):
    with pytest.raises(ValidationError, match='string_pattern'):
        _config(themis={'prod': _settings('prod', acl_secret=acl_secret)})


@pytest.mark.parametrize(
    'acl_secret',
    [
        'projects/abcdef/secrets/acl',
        'projects/' + 'a' * 30 + '/secrets/acl',
        'projects/themis-prod-1/secrets/' + 'Az09_-' * 42 + 'abc',
    ],
)
def test_acl_secret_accepts_any_secret_resource_name(acl_secret: str):
    config = _config(themis={'prod': _settings('prod', acl_secret=acl_secret)})
    assert config.themis['prod'].acl_secret == acl_secret


@pytest.mark.parametrize(
    ('overrides', 'message'),
    [
        (
            {'sandbox_service_account': 'sandbox@project.iam.gserviceaccount.com.evil'},
            'string_pattern',
        ),
        ({'allowed_datasets': []}, 'too_short'),
        ({'namespace': 'main-upload'}, 'literal_error'),
        ({'namespace': 'Main'}, 'literal_error'),
        ({'allowed_datasets': ['ds-a', 'ds-a']}, 'duplicate'),
        # Themis's deploy grants its own auth service account read on the ACL
        ({'auth_service_account': _sa('auth-prod')}, 'extra_forbidden'),
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


@pytest.mark.parametrize(
    'field',
    ['acl_secret', 'sandbox_service_account', 'allowed_datasets', 'namespace'],
)
def test_settings_without_a_default_are_required(field: str):
    settings = _settings('prod')
    del settings[field]
    with pytest.raises(ValidationError, match='missing'):
        _config(themis={'prod': settings})


def test_environments_may_not_share_a_sandbox_service_account():
    shared = {'sandbox_service_account': _sa('sandbox')}
    with pytest.raises(ValidationError, match='share a sandbox service account'):
        _config(
            themis={
                'dev': _settings('dev', **shared),
                'prod': _settings('prod', **shared),
            },
        )


def test_environments_may_not_share_an_acl_secret():
    shared = {'acl_secret': _acl_secret('shared')}
    with pytest.raises(ValidationError, match='share an ACL secret'):
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
# region ACL


def test_acl_has_one_project_per_dataset_with_both_roles_as_lowercase_emails():
    assert _acl(['ds-a', 'ds-b']) == {
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
    acl = _acl(['ds-x'], {'ds-x': _dataset('ds-x', members=members)})
    assert ('ds-x' in acl['projects']) is is_project


def test_only_allowed_datasets_are_projects():
    assert set(_acl(ALL_DATASETS)['projects']) == {'ds-a', 'ds-b'}
    assert set(_acl(['ds-b', 'ds-c'])['projects']) == {'ds-b'}


def test_acl_without_projects_has_an_empty_projects_map():
    """Themis accepts an ACL with no Projects."""
    assert _acl(['ds-c', 'ds-d']) == {'format': 1, 'projects': {}}


def test_serialised_acl_is_deterministic_and_compact():
    """Config order does not reach the payload, so an unchanged config writes no
    new secret version."""
    reordered = {
        'ds-a': _dataset(
            'ds-a',
            members={'themis-curator': ['alice'], 'themis-manager': ['alice', 'bob']},
        ),
        'ds-b': _dataset('ds-b', members={'themis-curator': ['bob', 'carol']}),
    }
    serialised = serialise_acl(_acl(['ds-a', 'ds-b']))
    assert serialised == serialise_acl(_acl(['ds-b', 'ds-a'], reordered))
    assert json.loads(serialised) == _acl(['ds-a', 'ds-b'])
    # none of the ACL's keys or values contain whitespace
    assert not any(c.isspace() for c in serialised)


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
        _acl(['ds-x'], datasets)


def test_unconfigured_allowed_dataset_raises():
    with pytest.raises(ValueError, match="dataset 'ds-z', which is not configured"):
        _acl(['ds-a', 'ds-z'])


# endregion
# region driver

SECRET_VERSION_TYPE = 'gcp:secretmanager/secretVersion:SecretVersion'  # noqa: S105
# Secret Manager's limit on the payload of one secret version, in bytes
SECRET_VERSION_LIMIT = 65536


class _RecordingMocks(pulumi.runtime.Mocks):
    """Records each resource the program registers, as (type, inputs)."""

    def __init__(self) -> None:
        self.registered: list[tuple[str, dict[str, Any]]] = []

    def new_resource(
        self, args: pulumi.runtime.MockResourceArgs
    ) -> tuple[str | None, dict[str, Any]]:
        self.registered.append((args.typ, dict(args.inputs)))
        return f'{args.name}-id', dict(args.inputs)

    def call(self, args: pulumi.runtime.MockCallArgs) -> dict[str, Any]:  # noqa: ARG002
        return {}


@pytest.fixture
def registered() -> list[tuple[str, dict[str, Any]]]:
    """The resources registered with pulumi during the test, as (type, inputs)."""
    mocks = _RecordingMocks()
    pulumi.runtime.set_mocks(mocks, preview=False)
    return mocks.registered


def _make_root(
    config: CPGInfrastructureConfig,
    datasets: dict[str, CPGDatasetConfig] | None = None,
) -> tuple[CPGInfrastructure, dict[str, MagicMock]]:
    """A CPGInfrastructure with only what setup_themis touches.

    Constructing a real one walks the whole plugin and group machinery. The
    common infrastructure is real, so the resources it registers reach the
    `registered` fixture; the dataset infrastructures are mocks.
    """
    datasets = DATASETS if datasets is None else datasets
    root = CPGInfrastructure.__new__(CPGInfrastructure)
    root.config = config
    root.dataset_configs = datasets

    # common_gcp_infra is a cached_property; seed the cache
    root.__dict__['common_gcp_infra'] = GcpInfrastructure(
        config=config,
        context=InfraContext(name_prefix='common', gcp_project_id='common-1234'),
    )

    cloud_infras = {d: MagicMock(spec=CPGDatasetCloudInfrastructure) for d in datasets}
    root.dataset_infrastructures = {
        d: MagicMock(clouds={'gcp': cloud_infra})
        for d, cloud_infra in cloud_infras.items()
    }
    return root, cloud_infras


@pulumi.runtime.test
def _run(step: Callable[[], object]) -> None:
    """Run a step and wait until pulumi has registered its resources.

    Outside this, the mocks never see a registration, so `registered` stays
    empty whatever the step does.
    """
    step()


def _setup_themis(root: CPGInfrastructure) -> None:
    _run(root.setup_themis)


def _written(registered: list[tuple[str, dict[str, Any]]]) -> dict[str, Any]:
    """Secret resource name -> the ACL written to it as a new version.

    Fails on any other resource: the deploy only adds versions to secrets that
    Themis's deploy creates and grants on.
    """
    assert {typ for typ, _ in registered} <= {SECRET_VERSION_TYPE}
    written = {}
    for _, inputs in registered:
        assert inputs['secret'] not in written
        # secret_data is a secret input: pulumi wraps its value
        written[inputs['secret']] = json.loads(inputs['secretData']['value'])
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


def test_setup_themis_writes_each_environments_acl_and_grants_access(
    registered: list[tuple[str, dict[str, Any]]],
):
    root, cloud_infras = _make_root(_config())
    _setup_themis(root)

    # one secret version per environment, in the secret it configures, and no
    # bucket, secret or IAM binding
    assert [typ for typ, _ in registered] == [SECRET_VERSION_TYPE] * 2
    assert _written(registered) == {
        _acl_secret('dev'): _acl(ALL_DATASETS),
        _acl_secret('prod'): _acl(ALL_DATASETS),
    }

    # ds-c and ds-d are allowed, but no Project is built from them
    both = {_sa('sandbox-dev'), _sa('sandbox-prod')}
    assert _data_readers(cloud_infras) == {
        'ds-a': both,
        'ds-b': both,
        'ds-c': set(),
        'ds-d': set(),
    }
    # each environment's sandbox service account reads its own namespace
    namespaces = {
        (call.args[0], call.kwargs['environment'], call.kwargs['namespace'])
        for cloud_infra in cloud_infras.values()
        for call in cast('MagicMock', cloud_infra.add_themis_data_reader).call_args_list
    }
    assert namespaces == {
        (_sa('sandbox-dev'), 'dev', 'test'),
        (_sa('sandbox-prod'), 'prod', 'main'),
    }


def test_sandbox_service_account_reads_exactly_the_acl_datasets(
    registered: list[tuple[str, dict[str, Any]]],
):
    config = _config(
        themis={
            'dev': _settings('dev', allowed_datasets=['ds-a', 'ds-c']),
            'prod': _settings('prod', allowed_datasets=['ds-b', 'ds-d']),
        },
    )
    root, cloud_infras = _make_root(config)
    _setup_themis(root)

    readers = _data_readers(cloud_infras)
    for environment in ('dev', 'prod'):
        acl = _written(registered)[_acl_secret(environment)]
        assert {
            dataset
            for dataset, sas in readers.items()
            if _sa(f'sandbox-{environment}') in sas
        } == set(acl['projects'])


def test_environments_are_separate(registered: list[tuple[str, dict[str, Any]]]):
    """A dataset allowed in one environment reaches neither the other's ACL nor
    the other's sandbox service account."""
    config = _config(
        themis={
            'dev': _settings('dev', allowed_datasets=['ds-a']),
            'prod': _settings('prod', allowed_datasets=['ds-b']),
        },
    )
    root, cloud_infras = _make_root(config)
    _setup_themis(root)

    written = _written(registered)
    dev, prod = written[_acl_secret('dev')], written[_acl_secret('prod')]
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
        'ds-a': {_sa('sandbox-dev')},
        'ds-b': {_sa('sandbox-prod')},
        'ds-c': set(),
        'ds-d': set(),
    }


def test_setup_themis_is_a_no_op_without_themis_config(
    registered: list[tuple[str, dict[str, Any]]],
):
    root, cloud_infras = _make_root(_config(themis={}))
    root.validate_themis()
    _setup_themis(root)

    assert not registered
    for cloud_infra in cloud_infras.values():
        assert not cloud_infra.method_calls


DS_X = {'ds-x': _dataset('ds-x', members={'themis-curator': ['curator']})}


def _config_with_acl_of(size: int) -> CPGInfrastructureConfig:
    """A config whose one environment's ACL payload is `size` bytes: one
    Project, ds-x, whose one curator's email is padded to fit."""

    def users(local_part: str) -> dict[str, CPGInfrastructureUser]:
        return {'curator': _user('curator', f'{local_part}@example.com')}

    unpadded = len(serialise_acl(build_acl(['ds-x'], DS_X, users('c'))))
    padded = users('c' * (1 + size - unpadded))
    assert len(serialise_acl(build_acl(['ds-x'], DS_X, padded))) == size
    return _config(
        themis={'dev': _settings('dev', allowed_datasets=['ds-x'])},
        users=padded,
    )


def test_acl_at_the_secret_version_limit_is_written(
    registered: list[tuple[str, dict[str, Any]]],
):
    root, _ = _make_root(_config_with_acl_of(SECRET_VERSION_LIMIT), DS_X)
    _setup_themis(root)
    [(_, inputs)] = registered
    assert len(inputs['secretData']['value'].encode('utf-8')) == SECRET_VERSION_LIMIT


def test_acl_over_the_secret_version_limit_fails_before_any_resource_is_registered(
    registered: list[tuple[str, dict[str, Any]]],
):
    root, cloud_infras = _make_root(_config_with_acl_of(SECRET_VERSION_LIMIT + 1), DS_X)
    for step in (root.validate_themis, root.setup_themis):
        with pytest.raises(
            ValueError,
            match=(
                f"environment 'dev': its ACL is {SECRET_VERSION_LIMIT + 1} bytes, "
                f'over the {SECRET_VERSION_LIMIT}'
            ),
        ):
            _run(step)
    assert not registered
    cast('MagicMock', cloud_infras['ds-x'].add_themis_data_reader).assert_not_called()


def test_dataset_without_a_gcp_deploy_location_raises():
    root, _ = _make_root(_config())
    root.dataset_infrastructures['ds-b'].clouds = {}
    with pytest.raises(ValueError, match="'ds-b', which has no 'gcp' deploy location"):
        root.validate_themis()


def test_unreadable_dataset_fails_before_any_resource_is_registered(
    registered: list[tuple[str, dict[str, Any]]],
):
    """A partial deploy would publish an ACL whose data grants never landed."""
    root, cloud_infras = _make_root(_config())
    cast(
        'MagicMock', cloud_infras['ds-b'].check_themis_data_readable
    ).side_effect = ValueError('ds-b: Themis reads')

    for step in (root.validate_themis, root.setup_themis):
        with pytest.raises(ValueError, match='ds-b: Themis reads'):
            _run(step)
    assert not registered
    for cloud_infra in cloud_infras.values():
        cast('MagicMock', cloud_infra.add_themis_data_reader).assert_not_called()


def test_allowed_dataset_outside_the_acl_needs_no_themis_prerequisites():
    """Only the datasets Themis reads must be readable by it."""
    root, cloud_infras = _make_root(_config())
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
    driver.add_themis_data_reader(
        _sa('sandbox-x'), environment='x', namespace=namespace
    )

    provider_groups = driver.group_provider.groups['gcp']
    joined = {
        name
        for name, g in provider_groups.items()
        if _sa('sandbox-x') in _direct_members(g)
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
    sandbox_sa = _sa('sandbox-x')

    driver.add_themis_data_reader(sandbox_sa, environment='x', namespace=namespace)
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
        name for name, b in buckets.items() if sandbox_sa in _bucket_readers(driver, b)
    }
    assert read == {bucket_name}
    # update_metamist_members sends this group's members as the project's readers
    resolved = {
        kind: {m.cloud_id for m in provider.resolve_group_members(group)}
        for kind, group in driver.metamist_groups.items()
    }
    assert {kind for kind, members in resolved.items() if sandbox_sa in members} == {
        metamist_read,
    }
    assert sandbox_sa in {m.cloud_id for m in provider.resolve_group_members(invokers)}


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
            _sa('sandbox-x'), environment='x', namespace=namespace
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
