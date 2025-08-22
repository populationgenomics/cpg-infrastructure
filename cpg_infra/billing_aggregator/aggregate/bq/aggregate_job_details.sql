CREATE OR REPLACE MATERIALIZED VIEW `billing-admin-290403.billing_aggregate.aggregate_job_details`
AS
SELECT
    JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.ar-guid') as ar_guid,
    JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.batch_id') as batch_id,
    CASE WHEN JSON_QUERY(labels, '$.sequencing_groups') IS NOT NULL THEN ARRAY_TO_STRING(JSON_VALUE_ARRAY(labels, '$.sequencing_groups'), ',')
    ELSE COALESCE(JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.sequencing_group'), JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.sequencing-group'))
    END as sequencing_group,
    CASE WHEN JSON_QUERY(labels, '$.cohorts') IS NOT NULL THEN ARRAY_TO_STRING(JSON_VALUE_ARRAY(labels, '$.cohorts'), ',') ELSE NULL END as cohorts,
    MIN(DATE_TRUNC(usage_start_time, DAY)) as min_day,
    MAX(DATE_TRUNC(usage_end_time, DAY)) as max_day
    FROM `billing-admin-290403.billing_aggregate.aggregate`
    WHERE cost_type <> 'tax'
    GROUP BY ar_guid, batch_id, sequencing_group, cohorts
