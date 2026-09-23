"""Behavioural tests for the Seqera driver integration."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import TestCase
from unittest.mock import MagicMock

from cpg_infra.config import (
    CPGDatasetConfig,
    CPGInfrastructureConfig,
)

if TYPE_CHECKING:
    from cpg_infra.driver.dataset_cloud_infrastructure import (
        CPGDatasetCloudInfrastructure,
    )


def _make_dataset_config(
    *,
    team_ownership: str | None,
    components: list[str],
) -> CPGDatasetConfig:
    return CPGDatasetConfig.model_validate(
        {
            'dataset': 'DATASET',
            'budgets': {},
            'gcp': {'project': 'dataset-1234'},
            'team_ownership': team_ownership,
            'components': {'gcp': components},
        },
    )


class TestShouldSetupSeqeraGate(TestCase):
    """The should_setup_seqera gate on CPGDatasetCloudInfrastructure."""

    def _make_driver(
        self,
        *,
        team_ownership: str | None,
        components: list[str],
        seqera_configured: bool = True,
        infra_is_gcp: bool = True,
    ) -> CPGDatasetCloudInfrastructure:
        from cpg_infra.abstraction.gcp import GcpInfrastructure
        from cpg_infra.driver.dataset_cloud_infrastructure import (
            CPGDatasetCloudInfrastructure,
        )

        infra = MagicMock(spec=GcpInfrastructure if infra_is_gcp else object)
        infra.name.return_value = 'gcp' if infra_is_gcp else 'azure'

        infra_config = MagicMock(spec=CPGInfrastructureConfig)
        infra_config.seqera = MagicMock() if seqera_configured else None

        dataset_config = _make_dataset_config(
            team_ownership=team_ownership,
            components=components,
        )
        return CPGDatasetCloudInfrastructure(
            config=infra_config,
            root=MagicMock(),
            group_provider=MagicMock(),
            infra=infra,
            dataset_config=dataset_config,
        )

    def test_gate_true_when_component_and_team_and_gcp(self):
        driver = self._make_driver(
            team_ownership='Rare Disease',
            components=['seqera-accounts'],
        )
        self.assertTrue(driver.should_setup_seqera)

    def test_gate_false_when_component_absent(self):
        driver = self._make_driver(
            team_ownership='Rare Disease',
            components=['storage'],
        )
        self.assertFalse(driver.should_setup_seqera)

    def test_component_without_team_ownership_raises(self):
        """SEQERA_ACCOUNTS in components + missing team_ownership = config error"""
        with self.assertRaises(ValueError) as ctx:
            self._make_driver(
                team_ownership=None,
                components=['seqera-accounts'],
            )
        self.assertIn('team_ownership', str(ctx.exception))

    def test_component_without_global_seqera_config_raises(self):
        """SEQERA_ACCOUNTS in components + no CPGInfrastructureConfig.seqera = config error"""
        with self.assertRaises(ValueError) as ctx:
            self._make_driver(
                team_ownership='Rare Disease',
                components=['seqera-accounts'],
                seqera_configured=False,
            )
        self.assertIn('seqera', str(ctx.exception).lower())
