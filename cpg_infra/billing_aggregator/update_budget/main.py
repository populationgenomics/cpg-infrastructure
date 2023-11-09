# pylint: disable=import-error,no-name-in-module,unused-argument
"""A Cloud Function to collect budget information from projects billing
and store it in the bq table.
"""

import json
import os
import functions_framework
import logging
from functools import cache
from flask import Request
import google.cloud.billing.budgets_v1.services.budget_service as budget
import google.cloud.bigquery as bq


BILLING_ACCOUNT_ID = os.getenv('BILLING_ACCOUNT_ID')
BILLING_MONTHLY_BUDGET_TABLE = os.getenv('BQ_BILLING_MONTHLY_BUDGET_TABLE') 

assert BILLING_ACCOUNT_ID, BILLING_MONTHLY_BUDGET_TABLE

logger = logging.getLogger('update-budget')


@cache
def get_bigquery_client() -> bq.Client:
    """Get instantiated cached bq client"""
    return bq.Client()


@cache
def get_budget_client() -> budget.BudgetServiceClient:
    """Get instantiated cached budget client"""
    return budget.BudgetServiceClient()


def try_cast_int(i):
    """Cast i to int, else return None if ValueError"""
    try:
        return int(i)
    except ValueError:
        return None


def insert_new_budget(bg_table, bq_client, gcp_projet, budget, currency):
    new_budget_obj = {
        'gcp_project': gcp_projet,
        'budget': budget,
        'currency': currency,
    }

    errors = bq_client.insert_rows_json(
        bg_table, [new_budget_obj]
    )
    if errors:
        # log errors
        logger.error(f'Error: {errors} when inserting {new_budget_obj}')
        return errors
    
    return None


def compare_and_update_stored_budget(bg_table, bq_client, budget_rec, stored_budgets):
    inner_amount = budget_rec.amount.specified_amount
    if not inner_amount:
        # ignore, project has no set budget
        return None

    budget_currency = inner_amount.currency_code
    budget_total = try_cast_int(inner_amount.units)

    # is gcp_project budget already stored, if not than insert
    to_be_inserted = budget_rec.display_name not in stored_budgets
    res = None

    if not to_be_inserted:
        # record exist, compare stored vs actual budget
        stored_rec = stored_budgets[budget_rec.display_name]
        to_be_inserted = (
            stored_rec.budget != budget_total or stored_rec.currency != budget_currency
        )

    if to_be_inserted:
        # insert required
        logger.info(f'Inserting new budget for {budget_rec.display_name}')
        res = insert_new_budget(
            bg_table, bq_client, budget_rec.display_name, budget_total, budget_currency
        )

    return res


@functions_framework.http
def from_request(request: Request):
    """Entrypoint for cloud functions, run always as default"""

    budget_client = get_budget_client()
    budgets = budget_client.list_budgets(parent=f'billingAccounts/{BILLING_ACCOUNT_ID}')

    bq_client = get_bigquery_client()
    query = f"""
        SELECT gcp_project, budget, currency
        FROM `{BILLING_MONTHLY_BUDGET_TABLE}`
    """
    stored_budgets = {b.gcp_project: b for b in list(bq_client.query(query).result())}
    res = []
    
    for b in budgets:
        errors = compare_and_update_stored_budget(
            BILLING_MONTHLY_BUDGET_TABLE, bq_client, b, stored_budgets
        )
        if errors:
            res.append({b.display_name: errors})

    if len(res) > 0:
        return {'success': False, 'errors': json.dumps(res)}, 500
    
    return {'success': True,}, 200
