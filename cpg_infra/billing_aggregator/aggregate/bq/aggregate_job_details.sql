CREATE OR REPLACE MATERIALIZED VIEW `billing-admin-290403.billing_aggregate.aggregate_job_details`
AS
SELECT
    JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.ar-guid') as ar_guid,
    JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.batch_id') as batch_id,
    MIN(DATE_TRUNC(usage_start_time, DAY)) as min_day,
    MAX(DATE_TRUNC(usage_end_time, DAY)) as max_day
    FROM `billing-admin-290403.billing_aggregate.aggregate`
    GROUP BY ar_guid, batch_id
