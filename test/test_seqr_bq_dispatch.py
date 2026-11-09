"""Tests for the Seqera / seqr-hosting BQ row dispatch in seqr.migrate_entries_from_bq."""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch

os.environ.setdefault('GCP_BILLING_SOURCE_TABLE', 'project.dataset.billing_test')
os.environ.setdefault('GCP_AGGREGATE_DEST_TABLE', 'project.dataset.aggregate_test')
os.environ.setdefault('BILLING_PROJECT_ID', 'project-test')

import importlib  # noqa: E402

import pandas as pd  # noqa: E402

# The aggregate package's __init__.py shadows the submodule attribute `seqr`
# with the from_pubsub function (`from .seqr import from_pubsub as seqr`),
# so we grab the actual module via importlib instead.
seqr_module = importlib.import_module('cpg_infra.billing_aggregator.aggregate.seqr')

HOSTING_MAP = [(date(2026, 1, 1), {'dsH': (1.0, 100)})]

_START = datetime(2026, 11, 7, tzinfo=UTC)
_END = datetime(2026, 11, 8, tzinfo=UTC)


def _row(
    *,
    cost: float,
    labels: list[dict[str, str]],
    ar_guid: str = 'arg-1',
    sku_id: str = 'sku-123',
    resource_id: str = 'res-1',
) -> dict:
    """Build a BQ billing-export row shaped like what seqr.py consumes."""
    sys_labels = [{'key': 'ar_guid', 'value': ar_guid}] if ar_guid else []
    ts = pd.Timestamp('2026-11-07 10:00:00')
    return {
        'service': {'id': 'compute-engine', 'description': 'Compute Engine'},
        'sku': {'id': sku_id, 'description': 'n1-standard-1 usage'},
        'usage_start_time': ts,
        'usage_end_time': ts,
        'export_time': ts,
        'labels': labels,
        'system_labels': sys_labels,
        'location': {'location': 'australia-southeast1'},
        'cost': cost,
        'currency': 'USD',
        'currency_conversion_rate': 1.0,
        'usage': {'amount': 1.0, 'unit': 'hour'},
        'credits': [],
        'invoice': {'month': '202611'},
        'cost_type': 'regular',
        'adjustment_info': None,
        # Unique bit so billing_obj_to_key differs per row
        'resource_id': resource_id,
    }


def _seqera_row(cost: float = 10.0, dataset: str = 'fewgenomes', ar_guid: str = 'arg-1',
                resource_id: str = 'res-1') -> dict:
    return _row(
        cost=cost,
        labels=[
            {'key': 'compute_category', 'value': 'nextflow'},
            {'key': 'dataset', 'value': dataset},
        ],
        ar_guid=ar_guid,
        resource_id=resource_id,
    )


def _hosting_row(cost: float = 5.0, resource_id: str = 'res-h') -> dict:
    return _row(cost=cost, labels=[], ar_guid='', resource_id=resource_id)


class SeqrBqDispatchTestBase(TestCase):
    """Shared fixture: mocks BigQuery + upsert, runs migrate_entries_from_bq in prod mode
    and captures the entries the function would insert.
    """

    def _run(self, rows: list[dict]) -> list[dict]:
        captured: list[dict] = []

        def _fake_upsert(table, objs, existing_ids, dry_run):
            captured.extend(objs)
            return len(objs)

        # Mock BQ chain: get_bigquery_client().query(...).result().to_dataframe_iterable()
        fake_df = pd.DataFrame(rows)
        fake_result = MagicMock()
        fake_result.to_dataframe_iterable.return_value = iter([fake_df])
        fake_query = MagicMock()
        fake_query.result.return_value = fake_result
        fake_client = MagicMock()
        fake_client.query.return_value = fake_query

        with patch.object(seqr_module.utils, 'get_bigquery_client', return_value=fake_client), \
             patch.object(seqr_module.utils, 'retrieve_stored_ids', return_value=set()), \
             patch.object(
                 seqr_module.utils,
                 'get_currency_conversion_rate_for_time',
                 return_value=1.0,
             ), \
             patch.object(
                 seqr_module.utils,
                 'upsert_rows_into_bigquery',
                 side_effect=_fake_upsert,
             ):
            seqr_module.migrate_entries_from_bq(
                start=_START,
                end=_END,
                prop_map=HOSTING_MAP,
                mode='prod',
                output_path=None,
            )

        return captured

    @staticmethod
    def _by_topic(entries: list[dict]) -> dict[str, list[dict]]:
        out: dict[str, list[dict]] = {}
        for e in entries:
            out.setdefault(e['topic'], []).append(e)
        return out


class TestSeqeraDispatch(SeqrBqDispatchTestBase):

    def test_seqera_test_namespace_dataset_folds_to_production_topic(self):
        entries = self._run([_seqera_row(cost=10.0, dataset='fewgenomes-test')])
        topics = self._by_topic(entries)

        # distributed entry should be on 'fewgenomes', not 'fewgenomes-test'
        self.assertIn('fewgenomes', topics)
        self.assertNotIn('fewgenomes-test', topics)

        distributed = [
            e for e in topics['fewgenomes']
            if not e['id'].endswith('-credit')
        ]
        self.assertEqual(1, len(distributed))
        self.assertAlmostEqual(10.0, distributed[0]['cost'])

    def test_seqera_production_dataset_attributes_full_cost(self):
        entries = self._run([_seqera_row(cost=10.0, dataset='fewgenomes')])
        topics = self._by_topic(entries)

        self.assertIn('fewgenomes', topics)
        distributed = [
            e for e in topics['fewgenomes']
            if not e['id'].endswith('-credit')
        ]
        self.assertEqual(1, len(distributed))
        self.assertAlmostEqual(10.0, distributed[0]['cost'])

    def test_seqera_seqr_dataset_attributes_to_seqr_topic(self):
        # When labels['dataset'] == 'seqr', the strip is a no-op and the
        # distributed entry lands on topic 'seqr' (alongside the original
        # seqr-topic entry and the seqr credit).
        entries = self._run([_seqera_row(cost=7.0, dataset='seqr')])
        topics = self._by_topic(entries)

        # all entries from one dataset='seqr' row must be on topic 'seqr'
        self.assertEqual({'seqr'}, set(topics.keys()))
        # exactly one credit entry
        credits_ = [e for e in topics['seqr'] if e['id'].endswith('-credit')]
        self.assertEqual(1, len(credits_))

    def test_seqera_row_emits_matching_seqr_credit(self):
        entries = self._run([_seqera_row(cost=10.0, dataset='fewgenomes')])
        credits_ = [e for e in entries if e['id'].endswith('-credit')]
        self.assertEqual(1, len(credits_))
        self.assertEqual('seqr', credits_[0]['topic'])
        self.assertAlmostEqual(-10.0, credits_[0]['cost'])

    def test_seqera_distributed_entry_preserves_compute_category_label(self):
        import rapidjson
        entries = self._run([_seqera_row(cost=10.0, dataset='fewgenomes')])
        distributed = [
            e for e in entries
            if e['topic'] == 'fewgenomes' and not e['id'].endswith('-credit')
        ]
        self.assertEqual(1, len(distributed))
        labels = rapidjson.loads(distributed[0]['labels'])
        self.assertEqual('nextflow', labels.get('compute_category'))

    def test_non_seqera_row_still_uses_hosting_prop_map(self):
        # usage_end_time is 2026-11-07, HOSTING_MAP has an entry at 2026-01-01 → matches.
        entries = self._run([_hosting_row(cost=5.0)])
        topics = self._by_topic(entries)

        # Hosting map routes to 'dsH' at 100%
        self.assertIn('dsH', topics)
        distributed = [e for e in topics['dsH'] if not e['id'].endswith('-credit')]
        self.assertEqual(1, len(distributed))
        self.assertAlmostEqual(5.0, distributed[0]['cost'])

    def test_two_rows_same_ar_guid_different_datasets_route_independently(self):
        entries = self._run([
            _seqera_row(cost=10.0, dataset='fewgenomes', ar_guid='shared', resource_id='r1'),
            _seqera_row(cost=4.0, dataset='acute-care', ar_guid='shared', resource_id='r2'),
        ])
        topics = self._by_topic(entries)

        self.assertIn('fewgenomes', topics)
        self.assertIn('acute-care', topics)

        fg = [e for e in topics['fewgenomes'] if not e['id'].endswith('-credit')]
        ac = [e for e in topics['acute-care'] if not e['id'].endswith('-credit')]
        self.assertEqual(1, len(fg))
        self.assertEqual(1, len(ac))
        self.assertAlmostEqual(10.0, fg[0]['cost'])
        self.assertAlmostEqual(4.0, ac[0]['cost'])

        # Two seqr credits, one per original row
        credits_ = [e for e in entries if e['id'].endswith('-credit')]
        self.assertEqual(2, len(credits_))
        self.assertEqual({'seqr'}, {c['topic'] for c in credits_})
