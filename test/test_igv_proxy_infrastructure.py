"""Behavioural tests for the IGV desktop proxy driver integration (SET-1249)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast
from unittest import TestCase
from unittest.mock import MagicMock

from cpg_infra.config import (
    CPGDatasetConfig,
    CPGInfrastructureConfig,
    CPGInfrastructureUser,
)
from cpg_infra.driver.constants import IGV_DESKTOP_ACCESS

if TYPE_CHECKING:
    from cpg_infra.driver.dataset_cloud_infrastructure import (
        CPGDatasetCloudInfrastructure,
    )
    from cpg_infra.driver.infrastructure import CPGInfrastructure

# Deliberately fictitious. The real proxy projects and service accounts live in
# cpg-infrastructure-private and must not be mirrored here.
PROD_SA = 'not-a-real-sa@nonexistent-prod-project.iam.gserviceaccount.com'
DEV_SA = 'not-a-real-sa@nonexistent-dev-project.iam.gserviceaccount.com'
PROD_PROJECT = 'nonexistent-prod-project'
DEV_PROJECT = 'nonexistent-dev-project'


def _make_igv_proxy_config(
    *,
    with_dev: bool = False,
) -> CPGInfrastructureConfig.IgvProxy:
    gcp: dict[str, Any] = {
        'prod': {
            'project': PROD_PROJECT,
            'server_machine_account': PROD_SA,
        },
    }
    if with_dev:
        gcp['dev'] = {
            'project': DEV_PROJECT,
            'server_machine_account': DEV_SA,
        }
    return CPGInfrastructureConfig.IgvProxy.model_validate({'gcp': gcp})


def _make_user(key: str, *, gcp_id: str | None) -> CPGInfrastructureUser:
    # no gcp_id => a user known to the system, but with no gcp identity
    clouds = (
        {'gcp': {'id': gcp_id}}
        if gcp_id
        else {'azure': {'id': f'{key}@azure.example.com'}}
    )
    return CPGInfrastructureUser.model_validate({'id': key, 'clouds': clouds})


def _make_dataset_config(
    dataset: str,
    *,
    igv_members: list[str] | None = None,
    setup_test: bool = True,
) -> CPGDatasetConfig:
    members: dict[str, list[str]] = {}
    if igv_members is not None:
        members[IGV_DESKTOP_ACCESS] = igv_members
    return CPGDatasetConfig.model_validate(
        {
            'dataset': dataset,
            'budgets': {},
            'gcp': {'project': f'{dataset}-1234'},
            'setup_test': setup_test,
            'members': members,
        },
    )


def _make_root(
    *,
    igv_proxy: CPGInfrastructureConfig.IgvProxy | None,
    dataset_configs: list[CPGDatasetConfig],
    users: dict[str, CPGInfrastructureUser] | None = None,
) -> CPGInfrastructure:
    """A CPGInfrastructure with only what generate_igv_proxy_config touches.

    Constructing a real one walks the whole plugin and group machinery.
    """
    from cpg_infra.abstraction.gcp import GcpInfrastructure
    from cpg_infra.driver.infrastructure import CPGInfrastructure

    config = MagicMock(spec=CPGInfrastructureConfig)
    config.igv_proxy = igv_proxy
    config.users = users or {
        'alice': _make_user('alice', gcp_id='alice@example.com'),
        'bob': _make_user('bob', gcp_id='bob@example.com'),
    }
    config.gcp = MagicMock()
    config.gcp.dataset_storage_prefix = 'cpg-'

    root = CPGInfrastructure.__new__(CPGInfrastructure)
    root.config = config
    root.dataset_configs = {d.dataset: d for d in dataset_configs}

    common_gcp_infra = MagicMock(spec=GcpInfrastructure)
    # mirror the real get_pulumi_name, which prefixes '{dataset}-{cloud}-';
    # a constant stub would hide resource-name collisions between the stacks
    common_gcp_infra.get_pulumi_name.side_effect = lambda key: f'common-gcp-{key}'
    # common_gcp_infra is a cached_property; seed the cache with the mock
    root.__dict__['common_gcp_infra'] = common_gcp_infra
    return root


def _gcp_infra(root: CPGInfrastructure) -> MagicMock:
    """The seeded common_gcp_infra mock, typed as a mock for assertions."""
    return cast('MagicMock', root.common_gcp_infra)


def _raw_payloads(root: CPGInfrastructure) -> dict[str, str]:
    """Map of secret-version resource key -> the serialised secret payload."""
    return {
        call.args[0]: call.kwargs['contents']
        for call in _gcp_infra(root).add_secret_version.call_args_list
    }


def _users(root: CPGInfrastructure, stack: str) -> dict[str, list[str]]:
    """The allow-list written for one proxy stack, as {email: [buckets]}."""
    payload = _raw_payloads(root)[f'igv-proxy-config-{stack}-latest']
    return json.loads(payload)['users']


class TestIgvProxySecretGeneration(TestCase):
    """The prod and dev allow-list secrets written by the driver."""

    def test_prod_payload_lists_main_buckets(self):
        """Maps user email -> the '-main' buckets they may read, sorted"""
        root = _make_root(
            igv_proxy=_make_igv_proxy_config(),
            dataset_configs=[
                _make_dataset_config('dataset-b', igv_members=['bob', 'alice']),
                _make_dataset_config('dataset-a', igv_members=['alice']),
                _make_dataset_config('dataset-c'),  # not participating
            ],
        )
        root.generate_igv_proxy_config()

        self.assertEqual(
            {
                'alice@example.com': ['cpg-dataset-a-main', 'cpg-dataset-b-main'],
                'bob@example.com': ['cpg-dataset-b-main'],
            },
            _users(root, 'prod'),
        )

    def test_prod_secret_written_to_prod_project_only(self):
        """One secret, in the prod project, readable by the prod SA"""
        root = _make_root(
            igv_proxy=_make_igv_proxy_config(),
            dataset_configs=[_make_dataset_config('dataset-a', igv_members=['alice'])],
        )
        root.generate_igv_proxy_config()

        gcp_infra = _gcp_infra(root)
        gcp_infra.create_secret.assert_called_once()
        self.assertEqual(
            PROD_PROJECT,
            gcp_infra.create_secret.call_args.kwargs['project'],
        )
        gcp_infra.add_secret_member.assert_called_once()
        self.assertEqual(
            PROD_SA,
            gcp_infra.add_secret_member.call_args.kwargs['member'],
        )

    def test_dev_payload_matches_prod_payload(self):
        """Both stacks read the same '-main' buckets, so they share a payload."""
        root = _make_root(
            igv_proxy=_make_igv_proxy_config(with_dev=True),
            dataset_configs=[
                _make_dataset_config('dataset-a', igv_members=['alice', 'bob']),
                _make_dataset_config('cohort-main-study', igv_members=['alice']),
            ],
        )
        root.generate_igv_proxy_config()

        expected = {
            'alice@example.com': [
                'cpg-cohort-main-study-main',
                'cpg-dataset-a-main',
            ],
            'bob@example.com': ['cpg-dataset-a-main'],
        }
        self.assertEqual(expected, _users(root, 'prod'))
        self.assertEqual(expected, _users(root, 'dev'))

    def test_no_test_bucket_is_ever_listed(self):
        """A dataset's test namespace is irrelevant: only '-main' is handed out."""
        root = _make_root(
            igv_proxy=_make_igv_proxy_config(with_dev=True),
            dataset_configs=[
                _make_dataset_config('dataset-a', igv_members=['alice']),
                _make_dataset_config(
                    'dataset-b',
                    igv_members=['bob'],
                    setup_test=False,
                ),
            ],
        )
        root.generate_igv_proxy_config()

        expected = {
            'alice@example.com': ['cpg-dataset-a-main'],
            'bob@example.com': ['cpg-dataset-b-main'],
        }
        self.assertEqual(expected, _users(root, 'prod'))
        self.assertEqual(expected, _users(root, 'dev'))
        for payload in _raw_payloads(root).values():
            self.assertNotIn('-test', payload)

    def test_payloads_are_deterministic(self):
        """The same input twice serialises byte-identically.

        Unsorted dict or list ordering would churn a new secret version on
        every deploy.
        """

        def run(datasets: list[CPGDatasetConfig]) -> dict[str, str]:
            root = _make_root(
                igv_proxy=_make_igv_proxy_config(with_dev=True),
                dataset_configs=datasets,
            )
            root.generate_igv_proxy_config()
            return _raw_payloads(root)

        a = _make_dataset_config('dataset-a', igv_members=['alice', 'bob'])
        b = _make_dataset_config('dataset-b', igv_members=['bob', 'alice'])
        self.assertEqual(run([a, b]), run([b, a]))

    def test_secrets_use_distinct_pulumi_resource_keys(self):
        """Both secrets share a secret_id, so they need distinct resource keys"""
        root = _make_root(
            igv_proxy=_make_igv_proxy_config(with_dev=True),
            dataset_configs=[_make_dataset_config('dataset-a', igv_members=['alice'])],
        )
        root.generate_igv_proxy_config()

        gcp_infra = _gcp_infra(root)
        for call_list in (
            gcp_infra.create_secret.call_args_list,
            gcp_infra.add_secret_version.call_args_list,
            gcp_infra.add_secret_member.call_args_list,
        ):
            keys = [
                call.kwargs.get('resource_key') or call.args[0] for call in call_list
            ]
            self.assertEqual(2, len(keys))
            self.assertEqual(len(keys), len(set(keys)))

        self.assertEqual(
            {'igv-proxy-config'},
            {c.kwargs['name'] for c in gcp_infra.create_secret.call_args_list},
        )

    def test_empty_allow_list_is_still_written(self):
        """No participating datasets still writes an empty allow-list.

        The proxy reads this secret unconditionally; leaving a stale version in
        place would be worse than an explicit empty map.
        """
        root = _make_root(
            igv_proxy=_make_igv_proxy_config(),
            dataset_configs=[_make_dataset_config('dataset-a')],
        )
        root.generate_igv_proxy_config()

        self.assertEqual({}, _users(root, 'prod'))

    def test_no_op_when_igv_proxy_absent(self):
        root = _make_root(
            igv_proxy=None,
            dataset_configs=[_make_dataset_config('dataset-a', igv_members=['alice'])],
        )
        root.generate_igv_proxy_config()

        gcp_infra = _gcp_infra(root)
        gcp_infra.create_secret.assert_not_called()
        gcp_infra.add_secret_version.assert_not_called()
        gcp_infra.add_secret_member.assert_not_called()

    def test_unknown_member_key_raises(self):
        root = _make_root(
            igv_proxy=_make_igv_proxy_config(),
            dataset_configs=[
                _make_dataset_config('dataset-a', igv_members=['not-a-user']),
            ],
        )
        with self.assertRaises(ValueError) as ctx:
            root.generate_igv_proxy_config()
        self.assertIn('not-a-user', str(ctx.exception))

    def test_member_without_gcp_identity_raises(self):
        root = _make_root(
            igv_proxy=_make_igv_proxy_config(),
            dataset_configs=[_make_dataset_config('dataset-a', igv_members=['carol'])],
            users={'carol': _make_user('carol', gcp_id=None)},
        )
        with self.assertRaises(ValueError) as ctx:
            root.generate_igv_proxy_config()
        self.assertIn('carol', str(ctx.exception))
        self.assertIn('gcp', str(ctx.exception))


class TestIgvProxyBucketBindings(TestCase):
    """The per-dataset bucket IAM bindings for the proxy service accounts."""

    def _make_driver(
        self,
        *,
        igv_proxy: CPGInfrastructureConfig.IgvProxy | None,
        igv_members: list[str] | None,
        infra_is_gcp: bool = True,
    ) -> CPGDatasetCloudInfrastructure:
        from cpg_infra.abstraction.azure import AzureInfra
        from cpg_infra.abstraction.gcp import GcpInfrastructure
        from cpg_infra.driver.dataset_cloud_infrastructure import (
            CPGDatasetCloudInfrastructure,
        )

        infra = MagicMock(spec=GcpInfrastructure if infra_is_gcp else AzureInfra)
        infra.name.return_value = 'gcp' if infra_is_gcp else 'azure'

        config = MagicMock(spec=CPGInfrastructureConfig)
        config.igv_proxy = igv_proxy
        config.web_service = None

        return CPGDatasetCloudInfrastructure(
            config=config,
            root=MagicMock(),
            group_provider=MagicMock(),
            infra=infra,
            dataset_config=_make_dataset_config('dataset-a', igv_members=igv_members),
        )

    @staticmethod
    def _bindings(driver: CPGDatasetCloudInfrastructure) -> dict[str, Any]:
        """Map of binding resource key -> member, for igv bindings only."""
        infra = cast('MagicMock', driver.infra)
        bindings = {}
        for call in infra.add_member_to_bucket.call_args_list:
            key, *positional = call.args
            if 'igv' not in key:
                continue
            # bucket/member/membership are passed positionally
            bindings[key] = positional[1]
        return bindings

    def test_main_bucket_bindings_follow_the_stack_config(self):
        """The prod SA always, the dev SA too once a dev stack is configured.

        Both bindings are on the same bucket, so their resource keys must differ.
        """
        cases: list[tuple[bool, set[str]]] = [
            (False, {PROD_SA}),
            (True, {DEV_SA, PROD_SA}),
        ]
        for with_dev, expected in cases:
            with self.subTest(with_dev=with_dev):
                driver = self._make_driver(
                    igv_proxy=_make_igv_proxy_config(with_dev=with_dev),
                    igv_members=['alice'],
                )
                driver.setup_storage_main_bucket_permissions()

                bindings = self._bindings(driver)
                self.assertEqual(expected, set(bindings.values()))
                self.assertEqual(len(expected), len(bindings))

    def test_no_bindings_on_the_test_buckets(self):
        """Neither stack gets test-namespace access."""
        driver = self._make_driver(
            igv_proxy=_make_igv_proxy_config(with_dev=True),
            igv_members=['alice'],
        )
        driver.setup_storage_test_buckets_permissions()
        self.assertEqual({}, self._bindings(driver))

    def test_no_bindings_when_dataset_does_not_participate(self):
        """Participation needs GCP, an igv_proxy block, and listed members"""
        cases = {
            'no members listed': {
                'igv_proxy': _make_igv_proxy_config(with_dev=True),
                'igv_members': None,
            },
            'igv_proxy absent': {'igv_proxy': None, 'igv_members': ['alice']},
            'non-gcp infrastructure': {
                'igv_proxy': _make_igv_proxy_config(with_dev=True),
                'igv_members': ['alice'],
                'infra_is_gcp': False,
            },
        }
        setups = (
            'setup_storage_main_bucket_permissions',
            'setup_storage_test_buckets_permissions',
        )
        for label, kwargs in cases.items():
            for setup in setups:
                with self.subTest(label, setup=setup):
                    driver = self._make_driver(**kwargs)  # type: ignore[arg-type]
                    getattr(driver, setup)()
                    self.assertEqual({}, self._bindings(driver))
