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
from cpg_infra.driver.groups import GroupProvider
from cpg_infra.driver.infrastructure import CPGInfrastructure
from cpg_infra.driver.themis import build_acl, serialise_acl

if TYPE_CHECKING:
    from collections.abc import Callable


# Deliberately fictitious; the real accounts live in cpg-infrastructure-private.
def _sa(name: str) -> str:
    return f'not-a-real-{name}@nonexistent-project.iam.gserviceaccount.com'


def _project(environment: str) -> str:
    return f'not-a-real-themis-{environment}'


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
        'no-gcp': None,
        'not-an-email': 'some-id-without-an-at-sign',
        'non-ascii': 'zoë@example.com',
        'robot': 'Robot@some-project.iam.gserviceaccount.com',
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
        members={'themis-manager': ['bob', 'alice'], 'themis-curator': ['alice']},
    ),
    # curators only
    'ds-b': _dataset('ds-b', members={'themis-curator': ['carol', 'bob']}),
    # no Themis role list, so never a Project
    'ds-c': _dataset('ds-c', members={'web-access': ['bob']}),
    # empty Themis role lists, so never a Project
    'ds-d': _dataset('ds-d', members={'themis-manager': [], 'themis-curator': []}),
}


def _settings(environment: str, **overrides: Any) -> dict[str, Any]:
    return {
        'project': _project(environment),
        'auth_service_account': _sa(f'auth-{environment}'),
        'sandbox_service_account': _sa(f'sandbox-{environment}'),
        'namespace': 'test' if environment == 'dev' else 'main',
        'allowed_datasets': sorted(DATASETS),
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


# region config and ACL


@pytest.mark.parametrize(
    ('shared', 'message'),
    [
        ({'sandbox_service_account': _sa('sandbox')}, 'a sandbox service account'),
        ({'project': _project('shared')}, 'a project'),
    ],
)
def test_environments_may_not_share_a_sandbox_service_account_or_project(
    shared: dict[str, str],
    message: str,
):
    with pytest.raises(ValidationError, match=f'must not share {message}'):
        _config(
            themis={
                'dev': _settings('dev', **shared),
                'prod': _settings('prod', **shared),
            },
        )


def test_serialised_acl_does_not_depend_on_config_order():
    """So an unchanged config writes no new secret version."""
    assert serialise_acl(build_acl(['ds-a', 'ds-b'], DATASETS, USERS)) == (
        serialise_acl(build_acl(['ds-b', 'ds-a'], DATASETS, USERS))
    )


@pytest.mark.parametrize(
    ('member', 'message'),
    [
        ('eve', "member 'eve' not found"),
        ('no-gcp', "'no-gcp' has no Google account"),
        ('not-an-email', 'not the email of a Google account'),
        ('non-ascii', 'not the email of a Google account'),
        ('robot', "'robot' is the service account"),
    ],
)
def test_unresolvable_member_raises(member: str, message: str):
    datasets = {'ds-x': _dataset('ds-x', members={'themis-curator': [member]})}
    with pytest.raises(ValueError, match=f'ds-x themis-curator: .*{message}'):
        build_acl(['ds-x'], datasets, USERS)


def test_unconfigured_allowed_dataset_raises():
    with pytest.raises(ValueError, match="dataset 'ds-z', which is not configured"):
        build_acl(['ds-a', 'ds-z'], DATASETS, USERS)


# endregion
# region setup_themis

SECRET_TYPE = 'gcp:secretmanager/secret:Secret'  # noqa: S105
SECRET_VERSION_TYPE = 'gcp:secretmanager/secretVersion:SecretVersion'  # noqa: S105
SECRET_IAM_MEMBER_TYPE = 'gcp:secretmanager/secretIamMember:SecretIamMember'  # noqa: S105
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
        if args.typ == SECRET_TYPE:
            # a Secret's id is its resource name
            inputs = args.inputs
            return f'projects/{inputs["project"]}/secrets/{inputs["secretId"]}', inputs
        return f'{args.name}-id', dict(args.inputs)

    def call(self, args: pulumi.runtime.MockCallArgs) -> dict[str, Any]:  # noqa: ARG002
        return {}


@pytest.fixture
def registered() -> list[tuple[str, dict[str, Any]]]:
    """The resources registered with pulumi during the test, as (type, inputs)."""
    mocks = _RecordingMocks()
    pulumi.runtime.set_mocks(mocks, preview=False)
    return mocks.registered


@pulumi.runtime.test
def _run(step: Callable[[], object]) -> None:
    """Run a step and wait until pulumi has registered its resources."""
    step()


def _make_root(
    config: CPGInfrastructureConfig,
    datasets: dict[str, CPGDatasetConfig] = DATASETS,
) -> tuple[CPGInfrastructure, dict[str, MagicMock]]:
    """A CPGInfrastructure with only what setup_themis touches.

    Constructing a real one walks the whole plugin and group machinery. The
    common infrastructure is real, so its resources reach `registered`; the
    dataset infrastructures are mocks.
    """
    root = CPGInfrastructure.__new__(CPGInfrastructure)
    root.config = config
    root.dataset_configs = datasets

    # common_gcp_infra and project_id are cached_properties; seed the caches
    common_gcp_infra = GcpInfrastructure(
        config=config,
        context=InfraContext(name_prefix='common', gcp_project_id='common-1234'),
    )
    common_gcp_infra.__dict__['project_id'] = 'common-1234'
    root.__dict__['common_gcp_infra'] = common_gcp_infra

    cloud_infras = {d: MagicMock(spec=CPGDatasetCloudInfrastructure) for d in datasets}
    root.dataset_infrastructures = {
        d: MagicMock(clouds={'gcp': cloud_infra})
        for d, cloud_infra in cloud_infras.items()
    }
    return root, cloud_infras


def _acl_secrets(
    registered: list[tuple[str, dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """Each created secret's resource name -> its ACL versions and IAM grants."""
    secrets: dict[str, dict[str, Any]] = {
        f'projects/{inputs["project"]}/secrets/{inputs["secretId"]}': {
            'versions': [],
            'grants': set(),
        }
        for typ, inputs in registered
        if typ == SECRET_TYPE
    }
    for typ, inputs in registered:
        if typ == SECRET_VERSION_TYPE:
            # secret_data is a secret input: pulumi wraps its value
            secrets[inputs['secret']]['versions'].append(
                json.loads(inputs['secretData']['value']),
            )
        elif typ == SECRET_IAM_MEMBER_TYPE:
            secrets[inputs['secretId']]['grants'].add(
                (inputs['project'], inputs['role'], inputs['member']),
            )
    return secrets


def _data_readers(cloud_infras: dict[str, MagicMock]) -> dict[str, set[tuple]]:
    """Dataset -> the (service account, namespace) pairs given read on it."""
    return {
        dataset: {
            (call.args[0], call.kwargs['namespace'])
            for call in cloud_infra.add_themis_data_reader.call_args_list
        }
        for dataset, cloud_infra in cloud_infras.items()
    }


def test_setup_themis_writes_each_environments_acl_and_grants_access(
    registered: list[tuple[str, dict[str, Any]]],
):
    config = _config(
        themis={
            'dev': _settings('dev', allowed_datasets=['ds-a', 'ds-c']),
            'prod': _settings('prod', allowed_datasets=['ds-b', 'ds-d']),
        },
    )
    root, cloud_infras = _make_root(config)
    _run(root.setup_themis)

    # each environment's `acl` secret, in its own project, holds one version:
    # the allowed datasets with a Themis role, both roles as sorted lowercase
    # emails; only its auth service account may read it
    projects = {
        'dev': {
            'ds-a': {
                'managers': ['alice.smith@example.org', 'bob@example.com'],
                'curators': ['alice.smith@example.org'],
            },
        },
        'prod': {
            'ds-b': {
                'managers': [],
                'curators': ['bob@example.com', 'carol@example.com'],
            },
        },
    }
    assert _acl_secrets(registered) == {
        f'projects/{_project(env)}/secrets/acl': {
            'versions': [{'format': 1, 'projects': projects[env]}],
            'grants': {
                (
                    _project(env),
                    'roles/secretmanager.secretAccessor',
                    f'serviceAccount:{_sa(f"auth-{env}")}',
                ),
            },
        }
        for env in ('dev', 'prod')
    }

    # each sandbox service account reads its own ACL's datasets in its namespace
    assert _data_readers(cloud_infras) == {
        'ds-a': {(_sa('sandbox-dev'), 'test')},
        'ds-b': {(_sa('sandbox-prod'), 'main')},
        'ds-c': set(),
        'ds-d': set(),
    }


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


def test_acl_at_the_secret_version_limit_is_accepted():
    root, _ = _make_root(_config_with_acl_of(SECRET_VERSION_LIMIT), DS_X)
    root.validate_themis()


def _acl_over_the_limit() -> tuple[CPGInfrastructure, dict[str, MagicMock]]:
    return _make_root(_config_with_acl_of(SECRET_VERSION_LIMIT + 1), DS_X)


def _dataset_without_gcp() -> tuple[CPGInfrastructure, dict[str, MagicMock]]:
    root, cloud_infras = _make_root(_config())
    root.dataset_infrastructures['ds-b'].clouds = {}
    return root, cloud_infras


def _unreadable_dataset() -> tuple[CPGInfrastructure, dict[str, MagicMock]]:
    root, cloud_infras = _make_root(_config())
    cloud_infras['ds-b'].check_themis_data_readable.side_effect = ValueError(
        'ds-b: Themis reads',
    )
    return root, cloud_infras


@pytest.mark.parametrize(
    ('make_root', 'message'),
    [
        (_acl_over_the_limit, f'its ACL is {SECRET_VERSION_LIMIT + 1} bytes'),
        (_dataset_without_gcp, "'ds-b', which has no 'gcp' deploy location"),
        (_unreadable_dataset, 'ds-b: Themis reads'),
    ],
)
def test_bad_config_fails_before_any_resource_is_registered(
    registered: list[tuple[str, dict[str, Any]]],
    make_root: Callable[[], tuple[CPGInfrastructure, dict[str, MagicMock]]],
    message: str,
):
    """A partial deploy would publish an ACL whose data grants never landed."""
    root, cloud_infras = make_root()
    for step in (root.validate_themis, root.setup_themis):
        with pytest.raises(ValueError, match=message):
            _run(step)
    assert not registered
    assert not any(_data_readers(cloud_infras).values())


# endregion
# region add_themis_data_reader


def _dataset_cloud_infra(
    dataset_config: CPGDatasetConfig,
) -> CPGDatasetCloudInfrastructure:
    infra = MagicMock(spec=GcpInfrastructure)
    infra.name.return_value = 'gcp'
    infra.get_pulumi_name.side_effect = lambda key: f'{dataset_config.dataset}-{key}'
    config = MagicMock(spec=CPGInfrastructureConfig)
    config.igv_proxy = None
    config.web_service = None
    return CPGDatasetCloudInfrastructure(
        config=config,
        root=MagicMock(),
        group_provider=GroupProvider(),
        infra=infra,
        dataset_config=dataset_config,
    )


def _bucket_readers(
    driver: CPGDatasetCloudInfrastructure,
    bucket: Any,
) -> set[str]:
    """The accounts with read on `bucket`, directly or through a group."""
    readers = set()
    for call in cast('MagicMock', driver.infra).add_member_to_bucket.call_args_list:
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
    ('namespace', 'metamist_read'),
    [('main', SM_MAIN_READ), ('test', SM_TEST_READ)],
)
def test_data_reader_reads_its_namespaces_bucket_and_may_invoke_metamist(
    namespace: ThemisNamespace,
    metamist_read: str,
):
    driver = _dataset_cloud_infra(
        _dataset('ds-a', components={'gcp': ['storage', 'metamist']}),
    )
    # a distinct mock per bucket, so the bucket a grant targets can be told apart
    cast('MagicMock', driver.infra).create_bucket.side_effect = (
        lambda name, **_: MagicMock(name=f'bucket-{name}')
    )
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
    assert read == {namespace}
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
        ('main', {'enable_metamist_project': False}, 'enable_metamist_project'),
        ('test', {'setup_test': False}, 'setup_test'),
    ],
)
def test_data_reader_needs_its_namespaces_storage_and_metamist(
    namespace: ThemisNamespace,
    extra: dict[str, Any],
    missing: str,
):
    driver = _dataset_cloud_infra(
        _dataset('ds-a', **{'components': {'gcp': ['storage', 'metamist']}, **extra}),
    )
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


# endregion
