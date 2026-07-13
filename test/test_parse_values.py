"""
Test module for checking the parsing of values in the config
"""

from typing import Any, Literal, Optional, TypedDict, TypeVar, Union
from unittest import TestCase

from cpg_infra.config import CPGDatasetConfig, CPGInfrastructureConfig
from cpg_infra.config.deserializabledataclass import try_parse_value_as_type


class _Settings(TypedDict, total=False):
    """A TypedDict used to check pass-through parsing."""

    who_can_post: str


# A non-class type used to check TypeError-free handling of exotic constructs.
T = TypeVar('T')


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

    def test_variable_length_tuple(self):
        """tuple[X, ...] coerces every element to X, regardless of length"""
        dtype = tuple[int, ...]
        self.assertTupleEqual((), try_parse_value_as_type([], dtype))
        self.assertTupleEqual((1,), try_parse_value_as_type(['1'], dtype))
        self.assertTupleEqual(
            (1, 2, 3),
            try_parse_value_as_type(['1', '2', '3'], dtype),
        )

    def test_variable_length_tuple_failure(self):
        """tuple[X, ...] rejects an element that can't be coerced to X"""
        dtype = tuple[int, ...]
        with self.assertRaises(ValueError):
            try_parse_value_as_type(['1', 'nope'], dtype)

    def test_typing_union(self):
        """typing.Union (not just `X | Y`) is routed through union parsing"""
        dtype = Union[int, str]
        self.assertEqual(1, try_parse_value_as_type('1', dtype))
        self.assertEqual('hello', try_parse_value_as_type('hello', dtype))

    def test_typing_optional_none(self):
        """Optional[X] accepts None"""
        dtype = Optional[int]
        self.assertIsNone(try_parse_value_as_type(None, dtype))
        self.assertEqual(1, try_parse_value_as_type('1', dtype))

    def test_union_failure_chains_cause(self):
        """A failed union coercion chains the first branch error as its cause"""
        with self.assertRaises(ValueError) as ctx:
            try_parse_value_as_type('hello', int | float)
        self.assertIsInstance(ctx.exception.__cause__, ValueError)

    def test_typeddict_passthrough(self):
        """A TypedDict is treated as a plain dict: any mapping passes, unvalidated"""
        value = {'who_can_post': 'ANYONE', 'unlisted_key': 123}
        self.assertDictEqual(value, try_parse_value_as_type(value, _Settings))

    def test_typeddict_rejects_non_dict(self):
        """A TypedDict field still rejects a non-dict value"""
        with self.assertRaises(ValueError):
            try_parse_value_as_type(['not', 'a', 'dict'], _Settings)

    def test_dataclass_instance_passthrough(self):
        """An already-deserialised dataclass instance passes through unchanged"""
        instance = try_parse_value_as_type(
            {'dataset': 'DATASET', 'budgets': {}, 'gcp': {'project': 'dataset-1234'}},
            CPGDatasetConfig,
        )
        self.assertIs(
            instance,
            try_parse_value_as_type(instance, CPGDatasetConfig),
        )

    def test_non_class_type_raises_value_error(self):
        """A non-class type (e.g. a TypeVar) raises ValueError, not TypeError"""
        with self.assertRaises(ValueError):
            try_parse_value_as_type(1, T)

    def test_union_with_non_class_branch_falls_through(self):
        """A non-class union branch is skipped so a valid branch still matches"""
        # a lone TypeVar branch is the point of this test
        dtype = Union[T, int]  # type: ignore[valid-type]
        self.assertEqual(1, try_parse_value_as_type('1', dtype))
