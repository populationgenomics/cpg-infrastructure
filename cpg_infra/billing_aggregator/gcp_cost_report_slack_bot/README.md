# GCP cost-report Slack bot

A Cloud Function (`slack_bot_cost_report` in `main.py`) that posts a daily
per-project GCP cost report to Slack. It reads billing data from BigQuery,
compares it against per-project budgets, flags projects that are over their
daily/monthly limits, and highlights projects whose billing account has been
**unlinked/disabled** (so they aren't forgotten and left at risk of data
deletion).

Production is deployed by Pulumi (`cpg_infra/billing_aggregator/driver.py`) on a
daily `0 9 * * *` schedule and posts to `#production-announcements`.

## Unlinked-billing highlighting

Projects with disabled billing are surfaced at the **top of the flagged-projects
list** every run. Their "Previous Day" column shows `💵⛓️‍💥 Billing unlinked`
while the "Month (%)" column keeps its normal value. The candidate set is the
union of every project with a budget and every project in the cost report; a
project is only flagged when the Cloud Billing API definitively reports billing
disabled (transient/permission errors are skipped and logged, never falsely
flagged). See `billing_status.py`.

## Manual testing (dev)

To iterate on changes, deploy a separate dev function (`gcp-cost-reporting-dev`)
that posts to `#sabrina-dev`. It runs as the same service account as
production, so it already has the billing/BigQuery permissions it needs, and it
is read-only on GCP (reads BigQuery, budgets and project billing info; posts to
Slack) — it cannot change any project's billing.

```bash
# 1. Deploy the dev function. It triggers off the existing 'cost-report'
#    Pub/Sub topic; production is NOT subscribed to that topic, so this is
#    isolated from #production-announcements.
./deploy.sh

# 2a. Daily-style run (flagged projects + any unlinked-billing projects):
#     publish any message to the trigger topic (the payload is ignored).
gcloud pubsub topics publish cost-report --message='{}' \
    --project=billing-admin-290403

# 2b. Full Monday-style dump via force_run: POST directly to the function's
#     Cloud Run URL (a gen2 function exposes one even with a Pub/Sub trigger).
URL=$(gcloud functions describe gcp-cost-reporting-dev \
    --region=australia-southeast1 --gen2 \
    --format='value(serviceConfig.uri)')
curl -m 600 -X POST "$URL" \
    -H "Authorization: bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d '{"force_run": true}'

# 3. Watch #sabrina-dev, and read logs:
gcloud functions logs read gcp-cost-reporting-dev \
    --region=australia-southeast1 --gen2 --limit=50

# 4. (Optional) tear down when finished.
gcloud functions delete gcp-cost-reporting-dev \
    --region=australia-southeast1 --gen2 --quiet
```

If the `curl` returns 403, grant yourself the invoker role on the function
(`roles/run.invoker` for gen2) and retry.
