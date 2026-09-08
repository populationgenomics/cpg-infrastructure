"""
Test module for validating the pydantic config models.
"""

from unittest import TestCase

from pydantic import ValidationError

from cpg_infra.config import (
    CPGDatasetComponents,
    CPGDatasetConfig,
    CPGInfrastructureConfig,
)


class TestConfigValidation(TestCase):
    """Validate construction and validation behaviour of the config models."""

    def test_billing_type(self):
        """Check that we can parse a billing config"""
        billing_config = {
            'coordinator_machine_account': 'aggregate-billing@iam.gserviceaccount.com',
            'hail_aggregator_username': 'billing-aggregator',
            'gcp': {
                'account_id': '<account-id>',
                'project_id': '<project-id>',
                'source_bq_table': 'billing.gcp_billing_export_v1_ABCDEF_123456_789ABC',
            },
            'gcp_cost_controls': {
                'timezone': 'Australia/Sydney',
                'machine_account': 'gcp-cost-control@billing-project.iam.gserviceaccount.com',
                'slack_channel': 'test-dev',
                'pubsub_topic': 'topic',
            },
            'aggregator': {
                'billing_sheet_id': '1a2b3c4d5e6f7g8h9i0j',
                'destination_bq_table': 'billing-project.billing_aggregate.aggregate',
                'functions': ['gcp', 'aws', 'azure'],
                'interval_hours': 4,
                'monthly_summary_table': 'billing-project.billing_aggregate.aggregate_monthly_cost',
                'slack_channel': 'software-alerts',
                'slack_token_secret_name': 'slack-aggregator-token',
            },
        }
        billing = CPGInfrastructureConfig.Billing.model_validate(billing_config)
        self.assertEqual('<project-id>', billing.gcp.project_id)
        self.assertEqual(4, billing.aggregator.interval_hours)

    def test_dataset_config_example(self):
        """Check that we can parse a minimal dataset config"""
        dataset_config = {
            'dataset': 'DATASET',
            'budgets': {},
            'gcp': {
                'project': 'dataset-1234',
            },
        }
        config = CPGDatasetConfig.model_validate(dataset_config)
        self.assertEqual('DATASET', config.dataset)
        self.assertEqual('dataset-1234', config.gcp.project)
        # defaults are applied
        self.assertEqual(['gcp'], config.deploy_locations)
        # metamist project labels default to unset
        self.assertIsNone(config.display_name)
        self.assertIsNone(config.description)

    def test_metamist_project_labels(self):
        """display_name and description are optional and parse when provided"""
        config = CPGDatasetConfig.model_validate(
            {
                'dataset': 'DATASET',
                'budgets': {},
                'gcp': {'project': 'dataset-1234'},
                'display_name': 'Rare Disease Cohort',
                'description': 'A cohort for rare disease research',
            },
        )
        self.assertEqual('Rare Disease Cohort', config.display_name)
        self.assertEqual('A cohort for rare disease research', config.description)

    def test_components_string_coercion(self):
        """Component strings are coerced into CPGDatasetComponents enum members"""
        config = CPGDatasetConfig.model_validate(
            {
                'dataset': 'DATASET',
                'budgets': {},
                'gcp': {'project': 'dataset-1234'},
                'components': {'gcp': ['storage', 'metamist']},
            },
        )
        self.assertEqual(
            [CPGDatasetComponents.STORAGE, CPGDatasetComponents.METAMIST],
            config.components['gcp'],
        )

    def test_extra_key_forbidden(self):
        """Unknown keys are rejected (extra='forbid')"""
        with self.assertRaises(ValidationError):
            CPGDatasetConfig.model_validate(
                {
                    'dataset': 'DATASET',
                    'budgets': {},
                    'gcp': {'project': 'dataset-1234'},
                    'not_a_real_field': True,
                },
            )

    def test_missing_required_field(self):
        """A missing required field raises a validation error"""
        with self.assertRaises(ValidationError):
            CPGDatasetConfig.model_validate({'dataset': 'DATASET', 'budgets': {}})

    def test_bad_literal_cloud_name(self):
        """A dict key outside the CloudName literal is rejected"""
        with self.assertRaises(ValidationError):
            CPGDatasetConfig.model_validate(
                {
                    'dataset': 'DATASET',
                    'budgets': {'not-a-cloud': {'monthly_budget': 100}},
                    'gcp': {'project': 'dataset-1234'},
                },
            )

    def test_frozen(self):
        """Config models are immutable"""
        config = CPGDatasetConfig.model_validate(
            {
                'dataset': 'DATASET',
                'budgets': {},
                'gcp': {'project': 'dataset-1234'},
            },
        )
        with self.assertRaises(ValidationError):
            config.dataset = 'OTHER'

    def test_direct_construction(self):
        """Nested models can be constructed directly with kwargs"""
        config = CPGDatasetConfig(
            dataset='fewgenomes',
            deploy_locations=['dry-run'],
            gcp=CPGDatasetConfig.Gcp(project='test-project'),
            budgets={'dry-run': CPGDatasetConfig.Budget(monthly_budget=100)},
        )
        self.assertEqual(100, config.budgets['dry-run'].monthly_budget)

    def test_seqera_account_model_parses(self):
        """SeqeraAccount round-trips minimal valid input"""
        from cpg_infra.config import SeqeraAccount

        account = SeqeraAccount(
            account_id='seqera-my-dataset-full',
            cloud_id='seqera-my-dataset-full@project.iam.gserviceaccount.com',
        )
        self.assertEqual('seqera-my-dataset-full', account.account_id)
        self.assertEqual(
            'seqera-my-dataset-full@project.iam.gserviceaccount.com',
            account.cloud_id,
        )

    def test_seqera_accounts_component_string_coercion(self):
        """The 'seqera-accounts' string coerces to the enum member"""
        config = CPGDatasetConfig.model_validate(
            {
                'dataset': 'DATASET',
                'budgets': {},
                'gcp': {'project': 'dataset-1234'},
                'components': {'gcp': ['seqera-accounts']},
            },
        )
        self.assertEqual(
            [CPGDatasetComponents.SEQERA_ACCOUNTS],
            config.components['gcp'],
        )

    def test_seqera_accounts_not_in_defaults(self):
        """SEQERA_ACCOUNTS is opt-in only — not in any default component set"""
        defaults = CPGDatasetComponents.default_component_for_infrastructure()
        for cloud, components in defaults.items():
            self.assertNotIn(
                CPGDatasetComponents.SEQERA_ACCOUNTS,
                components,
                f'SEQERA_ACCOUNTS should not be in default components for {cloud}',
            )

    def test_seqera_infra_config_parses(self):
        """CPGInfrastructureConfig.Seqera parses a minimal valid block"""
        seqera = CPGInfrastructureConfig.Seqera.model_validate(
            {
                'org_id': 12345,
                'wif_issuer_uri': 'https://cloud.seqera.io',
                'teams': {
                    'Rare Disease': {
                        'main': {'workspace_id': 111},
                        'test': {'workspace_id': 112},
                    },
                    'Population Genomics': {
                        'main': {'workspace_id': 222},
                        'test': {'workspace_id': 223},
                    },
                    'Shared': {
                        'main': {'workspace_id': 333},
                        'test': {'workspace_id': 334},
                    },
                },
            },
        )
        self.assertEqual(12345, seqera.org_id)
        self.assertEqual(111, seqera.teams['Rare Disease'].main.workspace_id)
        self.assertEqual(112, seqera.teams['Rare Disease'].test.workspace_id)

    def test_seqera_teams_reject_unknown_team(self):
        """teams with a key outside the TeamOwnership Literal must raise"""
        with self.assertRaises(ValidationError):
            CPGInfrastructureConfig.Seqera.model_validate(
                {
                    'org_id': 1,
                    'wif_issuer_uri': 'https://cloud.seqera.io',
                    'teams': {
                        'Rare-Disease': {  # note the hyphen typo
                            'main': {'workspace_id': 111},
                            'test': {'workspace_id': 112},
                        },
                    },
                },
            )

    def test_seqera_rejects_api_url_and_token_secret_name(self):
        """api_url and token_secret_name were moved to env vars — the model
        must reject them so config-vs-env drift fails fast at validation."""
        for extra_field, value in (
            ('api_url', 'https://cloud.seqera.io/api'),
            ('token_secret_name', 'secret/path'),
        ):
            with self.subTest(field=extra_field):
                with self.assertRaises(ValidationError) as ctx:
                    CPGInfrastructureConfig.Seqera.model_validate(
                        {
                            'org_id': 1,
                            'wif_issuer_uri': 'https://cloud.seqera.io',
                            extra_field: value,
                            'teams': {
                                'Rare Disease': {
                                    'main': {'workspace_id': 1},
                                    'test': {'workspace_id': 2},
                                },
                                'Population Genomics': {
                                    'main': {'workspace_id': 3},
                                    'test': {'workspace_id': 4},
                                },
                                'Shared': {
                                    'main': {'workspace_id': 5},
                                    'test': {'workspace_id': 6},
                                },
                            },
                        },
                    )
                self.assertIn(extra_field, str(ctx.exception))

    def test_seqera_optional_on_infrastructure_config(self):
        """CPGInfrastructureConfig.seqera defaults to None"""
        # Just verify the attribute exists on the class with the right default;
        # constructing a full CPGInfrastructureConfig here is heavy, so this
        # inspects the model field definition.
        field = CPGInfrastructureConfig.model_fields['seqera']
        self.assertIsNone(field.default)
