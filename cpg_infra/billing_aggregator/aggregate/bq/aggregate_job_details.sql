CREATE OR REPLACE MATERIALIZED VIEW `billing-admin-290403.billing_aggregate.aggregate_job_details`
AS
SELECT
    JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.ar-guid') as ar_guid,
    JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.batch_id') as batch_id,
    CASE WHEN JSON_QUERY(labels, '$.sequencing_groups') IS NOT NULL
    THEN
        -- sequencing_groups can be in 2 different formats
        UPPER(
            COALESCE(
                ARRAY_TO_STRING(JSON_VALUE_ARRAY(labels, '$.sequencing_groups'), ','),
                REGEXP_REPLACE(REPLACE(JSON_VALUE(PARSE_JSON(labels), '$.sequencing_groups'),"'",""), "\\[|\\]|[|]|\u0027,| |\"", "")
            )
        )
    ELSE
        -- otherwise again 2 different formats for singular key name
        UPPER(COALESCE(
            JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.sequencing_group'),
            JSON_VALUE(PARSE_JSON(labels, wide_number_mode=>'round'), '$.sequencing-group')
        ))
    END as sequencing_group,
    CASE WHEN JSON_QUERY(labels, '$.cohorts') IS NOT NULL
    THEN 
        ARRAY_TO_STRING(JSON_VALUE_ARRAY(labels, '$.cohorts'), ',')
    ELSE 
        NULL
    END as cohorts,
    invoice.month as invoice_month,
    MIN(DATE_TRUNC(usage_start_time, DAY)) as min_day,
    MAX(DATE_TRUNC(usage_end_time, DAY)) as max_day
    FROM `billing-admin-290403.billing_aggregate.aggregate`
    WHERE cost_type <> 'tax'
    GROUP BY ar_guid, batch_id, sequencing_group, cohorts, invoice_month
