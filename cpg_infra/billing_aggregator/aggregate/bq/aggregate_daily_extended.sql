CREATE OR REPLACE MATERIALIZED VIEW `billing-admin-290403.billing_aggregate.aggregate_daily_extended`
PARTITION BY DATE_TRUNC(day, DAY)
CLUSTER BY ar_guid, batch_id
AS
    SELECT DATE_TRUNC(usage_end_time, DAY) as day,
topic,
service.description as cost_category,
sku.description as sku,
invoice.month as invoice_month,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.ar-guid') as ar_guid,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.dataset') as dataset,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.batch_id') as batch_id,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.job_id') as job_id,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.sequencing_type') as sequencing_type,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.stage') as stage,
CASE WHEN JSON_QUERY(labels, '$.sequencing_groups') IS NOT NULL THEN REGEXP_REPLACE(REPLACE(JSON_VALUE(PARSE_JSON(labels), '$.sequencing_groups'),"'",""), "\\[|\\]", "")
ELSE COALESCE(JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.sequencing_group'), JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.sequencing-group'))
END as sequencing_group,
CASE WHEN JSON_QUERY(labels, '$.cohorts') IS NOT NULL THEN ARRAY_TO_STRING(JSON_VALUE_ARRAY(labels, '$.cohorts'), ',') ELSE NULL END as cohorts,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.compute-category') as compute_category,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.cromwell-sub-workflow-name') as cromwell_sub_workflow_name,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.cromwell-workflow-id') as cromwell_workflow_id,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.goog-pipelines-worker') as goog_pipelines_worker,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.wdl-task-name') as wdl_task_name,
JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.namespace') as namespace,
labels,
currency,
MIN(usage_start_time) as usage_start_time,
MAX(usage_end_time) as usage_end_time,
SUM(cost) as cost
FROM `billing-admin-290403.billing_aggregate.aggregate`
WHERE NOT REGEXP_CONTAINS(LOWER(service.description), r'credit') AND cost_type <> 'tax'
GROUP BY day, topic, cost_category, sku, invoice_month, ar_guid, dataset, batch_id, job_id, sequencing_type, stage, sequencing_group,
cohorts,
compute_category,
cromwell_sub_workflow_name,
cromwell_workflow_id,
goog_pipelines_worker,
wdl_task_name, namespace, labels, currency
