# Billing Aggregator

> Team docs: [Aggregated costs](https://github.com/populationgenomics/team-docs/blob/main/budgets.md#aggregated-costs)

As costs can occur in multiple environments, we redistribute these costs into topics - there is one topic per dataset (+ a couple of extra topics).

Here is the rough method for breaking these costs down:

1. We pass all cost from each GCP project (except seqr) onto the respective topic
1. For all hail batch projects except seqr, we calculate the Australian cost, and pass it onto the respective topic.
1. For seqr:
    - For costs in the seqr hail batch billing project:
        - If the job has a "dataset" attribute, we pass that cost onto the specific dataset directly
        - If the job does not, we distribute this across all seqr projects, based on the dataset's proportionate cram size for when the most relevant sample was added to metamist.
    - For all costs in the seqr GCP project, we distribute this across all seqr projects, based on the dataset's proportionate cram size for when a sequencing group was added to any elasticsearch index.

## Implementation

The Billing Aggregator [`driver.py`](driver.py) is implemented as a `CpgInfrastructurePlugin`.

There is one aggregator for each of the elements above, deployed in GCP cloud functions. The config in `cpg.toml` declares how frequently they get run, but as of writing it was once every 4 hours.

There are a couple of other functions that deployed by this plugin, a monthly_aggregator and an `update_budget`, useful for the metamist cost dashboard.
