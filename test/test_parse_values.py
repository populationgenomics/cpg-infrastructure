"""
Test module for checking the parsing of values in the config
"""

from typing import Any, Literal
from unittest import TestCase

from cpg_infra.config import CPGDatasetConfig, CPGInfrastructureConfig
from cpg_infra.config.deserializabledataclass import try_parse_value_as_type


class TestParseValues(TestCase):
    """Test the try_parse_value_as_type function"""

    def test_parse_any(self):
        """Check we allow any type"""
        dtype = Any
        self.assertEqual(1, try_parse_value_as_type(1, dtype))
        self.assertEqual('hello', try_parse_value_as_type('hello', dtype))
        self.assertEqual(None, try_parse_value_as_type(None, dtype))
        self.assertEqual([1, 2, 3], try_parse_value_as_type([1, 2, 3], dtype))
        self.assertEqual(
            {'hello': 'world'},
            try_parse_value_as_type({'hello': 'world'}, dtype),
        )

    def test_parse_string(self):
        """Basic string type checking"""
        dtype = str
        self.assertEqual('hello', try_parse_value_as_type('hello', dtype))

    def test_parse_str_or_int(self):
        """Check we allow either int | str"""
        dtype = int | str
        self.assertEqual('hello', try_parse_value_as_type('hello', dtype))
        self.assertEqual(1, try_parse_value_as_type('1', dtype))

    def test_parse_int_failure(self):
        """Check we fail if we don't specify an int"""
        dtype = int
        with self.assertRaises(ValueError):
            try_parse_value_as_type('hello', dtype)

    def test_parse_list_no_type(self):
        """Check we allow a list without a type"""
        dtype = list
        self.assertListEqual(['hello'], try_parse_value_as_type(['hello'], dtype))

    def test_parse_list_str_type(self):
        """Check we allow a list with a type"""
        dtype = list[str]
        self.assertListEqual(['hello'], try_parse_value_as_type(['hello'], dtype))

    def test_parse_list_str_failure(self):
        """Check we fail if we specify a list with a type that doesn't match"""
        dtype = list[int]
        with self.assertRaises(ValueError):
            try_parse_value_as_type(['hello'], dtype)

    def test_parse_list_union(self):
        """Check we allow a list with a union type"""
        dtype = list[int | str]
        self.assertListEqual([1, 'hello'], try_parse_value_as_type([1, 'hello'], dtype))

    def test_parse_list_union_failure(self):
        """Check we fail if we specify a list with a union type that doesn't match"""
        dtype = list[int | float]
        with self.assertRaises(ValueError):
            try_parse_value_as_type(['hello', 'world'], dtype)

    def test_parse_dict_no_type(self):
        """Check we allow a dict without a type"""
        dtype = dict
        self.assertDictEqual(
            {'hello': 'world'},
            try_parse_value_as_type({'hello': 'world'}, dtype),
        )

    def test_parse_dict_str_type(self):
        """Check we allow a dict with a type"""
        dtype = dict[str, str]
        self.assertDictEqual(
            {'hello': 'world'},
            try_parse_value_as_type({'hello': 'world'}, dtype),
        )

    def test_parse_dict_str_failure(self):
        """Check we fail if we specify a dict with a type that doesn't match"""
        dtype = dict[str, int]
        with self.assertRaises(ValueError):
            try_parse_value_as_type({'hello': 'hello'}, dtype)

    def test_parse_nested_dict(self):
        """Check we allow a nested dict"""
        dtype = dict[str, dict[str, str]]
        self.assertDictEqual(
            {'hello': {'world': 'hello'}},
            try_parse_value_as_type({'hello': {'world': 'hello'}}, dtype),
        )

    def test_parse_nested_dict_failure(self):
        """Check we fail if we specify a nested dict with a type that doesn't match"""
        dtype = dict[str, dict[str, int]]
        with self.assertRaises(ValueError):
            try_parse_value_as_type({'hello': {'world': 'hello'}}, dtype)

    def test_parse_tuple_no_type(self):
        """Check we allow a tuple without a type"""
        dtype = tuple
        self.assertTupleEqual(
            ('hello', 'world'),
            try_parse_value_as_type(('hello', 'world'), dtype),
        )

    def test_parse_tuple_with_type(self):
        """Check we allow a tuple with a type"""
        dtype = tuple[str, str]
        self.assertTupleEqual(
            ('hello', 'world'),
            try_parse_value_as_type(('hello', 'world'), dtype),
        )

    def test_parse_tuple_mismatched_length(self):
        """Check we fail if we specify a tuple with a type that doesn't match"""
        dtype = tuple[str, str, str]
        with self.assertRaises(ValueError):
            try_parse_value_as_type(('hello', 'world'), dtype)

    def test_parse_tuple_mismatched_type(self):
        """Check we fail if we specify a tuple with a type that doesn't match"""
        dtype = tuple[str, int]
        with self.assertRaises(ValueError):
            try_parse_value_as_type(('hello', 'world'), dtype)

    def test_billing_type(self):
        """Check that we can parse a billing config"""
        dtype = CPGInfrastructureConfig.Billing | None
        billing_config = {
            'coordinator_machine_account': 'aggregate-billing@iam.gserviceaccount.com',
            'billing_account_id': '01D012-20A6A2-CBD343',
            'hail_aggregator_username': 'billing-aggregator',
            'gcp': {
                'account_id': '<account-id>',
                'project_id': '<project-id>',
            },
            # TODO: adjust final values before committing
            'slack_bot': {
                'machine_account': 'gcp-cost-control@billing-admin-290403.iam.gserviceaccount.com',
                'slack_channel': 'sabrina-dev',
                'timezone': 'Australia/Sydney',
            },
        }
        _ = try_parse_value_as_type(billing_config, dtype)

    def test_optional_none(self):
        """Check that we can ignore parsing when optional is None"""
        dtype = CPGInfrastructureConfig.Billing | None
        billing_config = None
        _ = try_parse_value_as_type(billing_config, dtype)

    def test_dataset_config_example(self):
        """Check that we can parse a minimal dataset config"""
        dataset_config = {
            'dataset': 'DATASET',
            'budgets': {},
            'gcp': {
                'project': 'dataset-1234',
            },
        }
        _ = try_parse_value_as_type(dataset_config, CPGDatasetConfig)

    def test_subscripted(self):
        """
        Check that we can parse a subscripted type
        """
        _ = try_parse_value_as_type('hi', Literal['hi'])

    def test_literal_fail(self):
        """
        Check that we fail to parse a value not in the literal
        """
        with self.assertRaises(ValueError):
            _ = try_parse_value_as_type('hi', Literal['hello'])

    def test_any_pass(self):
        """
        Check that we can parse typing.Any
        """
        self.assertEqual(True, try_parse_value_as_type(True, Any))
        self.assertEqual(1, try_parse_value_as_type(1, Any))
        self.assertEqual('hi', try_parse_value_as_type('hi', Any))
        self.assertEqual({'hi': 'world'}, try_parse_value_as_type({'hi': 'world'}, Any))
