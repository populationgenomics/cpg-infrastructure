CREATE OR REPLACE MATERIALIZED VIEW `billing-admin-290403.billing_aggregate.aggregate_daily_cost_extended`
PARTITION BY DATE_TRUNC(day, DAY)
CLUSTER BY topic
AS
	SELECT DATE_TRUNC(usage_end_time, DAY) as day,
    topic,
    service.description as cost_category,
    sku.description as sku,
    invoice.month as invoice_month,
    CASE WHEN labels_record.key = 'ar-guid' THEN labels_record.value ELSE NULL END as ar_guid,
    CASE WHEN labels_record.key = 'dataset' THEN labels_record.value ELSE NULL END as dataset,
    CASE WHEN labels_record.key = 'batch_id' THEN labels_record.value ELSE NULL END as batch_id,
    CASE WHEN labels_record.key = 'sequencing_type' THEN labels_record.value ELSE NULL END as sequencing_type,
    CASE WHEN labels_record.key = 'stage' THEN labels_record.value ELSE NULL END as stage,
    CASE WHEN labels_record.key = 'sequencing-group' THEN labels_record.value ELSE NULL END as sequencing_group,
    CASE WHEN labels_record.key = 'compute-category' THEN labels_record.value ELSE NULL END as compute_category,
    CASE WHEN labels_record.key = 'cromwell-sub-workflow-name' THEN labels_record.value ELSE NULL END as cromwell_sub_workflow_name,
    CASE WHEN labels_record.key = 'cromwell-workflow-id' THEN labels_record.value ELSE NULL END as cromwell_workflow_id,
    CASE WHEN labels_record.key = 'goog-pipelines-worker' THEN labels_record.value ELSE NULL END as goog_pipelines_worker,
    CASE WHEN labels_record.key = 'wdl-task-name' THEN labels_record.value ELSE NULL END as wdl_task_name,
    CASE WHEN labels_record.key = 'namespace' THEN labels_record.value ELSE NULL END as namespace,
    currency,
    SUM(cost) as cost
    FROM `billing-admin-290403.billing_aggregate.aggregate_backup`
    CROSS JOIN UNNEST(labels) as labels_record
    WHERE labels_record.key in (
        'dataset', 'batch_id', 'sequencing_type', 'stage', 'sequencing-group', 'ar-guid', 'compute-category',
        'cromwell-sub-workflow-name', 'cromwell-workflow-id', 'goog-pipelines-worker', 'wdl-task-name', 'namespace'
    )
    AND NOT REGEXP_CONTAINS(LOWER(service.description), r'credit')
    GROUP BY day, topic, cost_category, sku, invoice_month, ar_guid, dataset, batch_id, sequencing_type, stage, sequencing_group,
    compute_category,
    cromwell_sub_workflow_name,
    cromwell_workflow_id,
    goog_pipelines_worker,
    wdl_task_name, namespace, currency
