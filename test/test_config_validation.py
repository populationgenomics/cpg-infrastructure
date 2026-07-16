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
