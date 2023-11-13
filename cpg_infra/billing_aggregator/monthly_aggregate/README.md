# Monthly Aggregate

Grabs the data from the montly invoice aggregate table and sends it to a spreadsheet


## Running on the VM
It is recommended that if you are loading/reloading data into the spreadsheets that you
refresh the materialized view in bigquery first using the following command:

```sql
CALL BQ.REFRESH_MATERIALIZED_VIEW('billing-admin-290403.billing_aggregate.aggregate_monthly_cost')
```
Set the location for running the query to the correct one under More > Query settings


If running on the VM use the following configuration:

```json
{
    "name": "Monthly Invoice",
    "type": "python",
    "request": "launch",
    "program": "${workspaceFolder}/cpg_infra/billing_aggregator/monthly_aggregate/main.py",
    "args": [],
    "console": "integratedTerminal",
    "justMyCode": true,
    "env": {
        "OUTPUT_BILLING_SHEET": "<spreadsheet_id>",
        "BQ_MONTHLY_SUMMARY_TABLE": "billing-admin-290403.billing_aggregate.aggregate_monthly_cost",
    }
}
```

This is how you [find the spreadsheet id](https://help.okta.com/wf/en-us/content/topics/workflows/connector-reference/googlesheets/actions/copyspreadsheet.htm)

In order for the run to work locally the `GOOGLE_APPLICATION_CREDETNIALS` environment variable needs to be set to the credentials for the service account `aggregate-billing@billing-admin-290403.iam.gserviceaccount.com`
