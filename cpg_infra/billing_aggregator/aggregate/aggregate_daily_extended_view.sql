CREATE OR REPLACE MATERIALIZED VIEW `billing-admin-290403.billing_aggregate.aggregate_daily_extended`
PARTITION BY DATE_TRUNC(day, DAY)
CLUSTER BY ar_guid
AS
    SELECT DATE_TRUNC(usage_end_time, DAY) as day,
    topic,
    service.description as cost_category,
    sku.description as sku,
    invoice.month as invoice_month,
    JSON_VALUE(PARSE_JSON(labels), '$.ar-guid') as ar_guid,
    JSON_VALUE(PARSE_JSON(labels), '$.dataset') as dataset,
    JSON_VALUE(PARSE_JSON(labels), '$.batch_id') as batch_id,
    JSON_VALUE(PARSE_JSON(labels), '$.job_id') as job_id,
    JSON_VALUE(PARSE_JSON(labels), '$.sequencing_type') as sequencing_type,
    JSON_VALUE(PARSE_JSON(labels), '$.stage') as stage,
    JSON_VALUE(PARSE_JSON(labels), '$.sequencing-group') as sequencing_group,
    JSON_VALUE(PARSE_JSON(labels), '$.compute-category') as compute_category,
    JSON_VALUE(PARSE_JSON(labels), '$.cromwell-sub-workflow-name') as cromwell_sub_workflow_name,
    JSON_VALUE(PARSE_JSON(labels), '$.cromwell-workflow-id') as cromwell_workflow_id,
    JSON_VALUE(PARSE_JSON(labels), '$.goog-pipelines-worker') as goog_pipelines_worker,
    JSON_VALUE(PARSE_JSON(labels), '$.wdl-task-name') as wdl_task_name,
    JSON_VALUE(PARSE_JSON(labels), '$.namespace') as namespace,
    labels,
    currency,
    MIN(usage_start_time) as usage_start_time,
    MAX(usage_end_time) as usage_end_time,
    SUM(cost) as cost
    FROM `billing-admin-290403.billing_aggregate.aggregate`
    WHERE NOT REGEXP_CONTAINS(LOWER(service.description), r'credit|distributed')
    GROUP BY day, topic, cost_category, sku, invoice_month, ar_guid, dataset, batch_id, job_id, sequencing_type, stage, sequencing_group,
    compute_category,
    cromwell_sub_workflow_name,
    cromwell_workflow_id,
    goog_pipelines_worker,
    wdl_task_name, namespace, labels, currency
