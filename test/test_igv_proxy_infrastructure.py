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

# Deliberately fictitious. The real proxy project and service account live in
# cpg-infrastructure-private and must not be mirrored here.
PROXY_SA = 'not-a-real-sa@nonexistent-proxy-project.iam.gserviceaccount.com'
PROXY_PROJECT = 'nonexistent-proxy-project'


def _make_igv_proxy_config() -> CPGInfrastructureConfig.IgvProxy:
    return CPGInfrastructureConfig.IgvProxy.model_validate(
        {
            'project': PROXY_PROJECT,
            'server_machine_account': PROXY_SA,
        },
    )


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
    # a constant stub would hide resource-name collisions
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


def _users(root: CPGInfrastructure) -> dict[str, list[str]]:
    """The allow-list written for the proxy, as {email: [buckets]}."""
    payload = _raw_payloads(root)['igv-proxy-config-latest']
    return json.loads(payload)['users']


class TestIgvProxyConfigValidation(TestCase):
    """Parsing and validation of the IgvProxy config models."""

    def test_igv_proxy_config_parses(self):
        """CPGInfrastructureConfig.IgvProxy parses a project and service account"""
        igv_proxy = CPGInfrastructureConfig.IgvProxy.model_validate(
            {
                'project': 'igv-proxy-prod',
                'server_machine_account': 'igv-prod@igv-proxy-prod.iam.gserviceaccount.com',
            },
        )
        self.assertEqual('igv-proxy-prod', igv_proxy.project)
        self.assertEqual(
            'igv-prod@igv-proxy-prod.iam.gserviceaccount.com',
            igv_proxy.server_machine_account,
        )

    def test_igv_proxy_optional_on_infrastructure_config(self):
        """CPGInfrastructureConfig.igv_proxy defaults to None"""
        field = CPGInfrastructureConfig.model_fields['igv_proxy']
        self.assertIsNone(field.default)

    def test_igv_desktop_access_member_key_parses(self):
        """A dataset can list members under the igv-desktop-access key"""
        config = CPGDatasetConfig.model_validate(
            {
                'dataset': 'DATASET',
                'budgets': {},
                'gcp': {'project': 'dataset-1234'},
                'members': {'igv-desktop-access': ['alice']},
            },
        )
        self.assertEqual(['alice'], config.members['igv-desktop-access'])


class TestIgvProxySecretGeneration(TestCase):
    """The allow-list secret written by the driver."""

    def test_payload_lists_main_buckets(self):
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
            _users(root),
        )

    def test_secret_written_to_proxy_project_only(self):
        """One secret, in the proxy project, readable by the proxy SA"""
        root = _make_root(
            igv_proxy=_make_igv_proxy_config(),
            dataset_configs=[_make_dataset_config('dataset-a', igv_members=['alice'])],
        )
        root.generate_igv_proxy_config()

        gcp_infra = _gcp_infra(root)
        gcp_infra.create_secret.assert_called_once()
        self.assertEqual(
            PROXY_PROJECT,
            gcp_infra.create_secret.call_args.kwargs['project'],
        )
        self.assertEqual(
            'igv-proxy-config',
            gcp_infra.create_secret.call_args.kwargs['name'],
        )
        gcp_infra.add_secret_member.assert_called_once()
        self.assertEqual(
            PROXY_SA,
            gcp_infra.add_secret_member.call_args.kwargs['member'],
        )
        self.assertEqual(
            PROXY_PROJECT,
            gcp_infra.add_secret_member.call_args.kwargs['project'],
        )

    def test_no_test_bucket_is_ever_listed(self):
        """A dataset's test namespace is irrelevant: only '-main' is handed out."""
        root = _make_root(
            igv_proxy=_make_igv_proxy_config(),
            dataset_configs=[
                _make_dataset_config('dataset-a', igv_members=['alice', 'bob']),
                _make_dataset_config('cohort-main-study', igv_members=['alice']),
                _make_dataset_config(
                    'dataset-b',
                    igv_members=['bob'],
                    setup_test=False,
                ),
            ],
        )
        root.generate_igv_proxy_config()

        self.assertEqual(
            {
                'alice@example.com': [
                    'cpg-cohort-main-study-main',
                    'cpg-dataset-a-main',
                ],
                'bob@example.com': ['cpg-dataset-a-main', 'cpg-dataset-b-main'],
            },
            _users(root),
        )
        for payload in _raw_payloads(root).values():
            self.assertNotIn('-test', payload)

    def test_payloads_are_deterministic(self):
        """The same input twice serialises byte-identically.

        Unsorted dict or list ordering would churn a new secret version on
        every deploy.
        """

        def run(datasets: list[CPGDatasetConfig]) -> dict[str, str]:
            root = _make_root(
                igv_proxy=_make_igv_proxy_config(),
                dataset_configs=datasets,
            )
            root.generate_igv_proxy_config()
            return _raw_payloads(root)

        a = _make_dataset_config('dataset-a', igv_members=['alice', 'bob'])
        b = _make_dataset_config('dataset-b', igv_members=['bob', 'alice'])
        self.assertEqual(run([a, b]), run([b, a]))

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

        self.assertEqual({}, _users(root))

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
    """The per-dataset bucket IAM binding for the proxy service account."""

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

    def test_main_bucket_binds_the_proxy_service_account(self):
        driver = self._make_driver(
            igv_proxy=_make_igv_proxy_config(),
            igv_members=['alice'],
        )
        driver.setup_storage_main_bucket_permissions()

        self.assertEqual(
            {'igv-proxy-main-bucket-read': PROXY_SA}, self._bindings(driver)
        )

    def test_no_bindings_on_the_test_buckets(self):
        """The proxy gets no test-namespace access."""
        driver = self._make_driver(
            igv_proxy=_make_igv_proxy_config(),
            igv_members=['alice'],
        )
        driver.setup_storage_test_buckets_permissions()
        self.assertEqual({}, self._bindings(driver))

    def test_no_bindings_when_dataset_does_not_participate(self):
        """Participation needs GCP, an igv_proxy block, and listed members"""
        cases = {
            'no members listed': {
                'igv_proxy': _make_igv_proxy_config(),
                'igv_members': None,
            },
            'igv_proxy absent': {'igv_proxy': None, 'igv_members': ['alice']},
            'non-gcp infrastructure': {
                'igv_proxy': _make_igv_proxy_config(),
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
