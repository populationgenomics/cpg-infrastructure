CREATE OR REPLACE MATERIALIZED VIEW `billing-admin-290403.billing_aggregate.aggregate_daily`
PARTITION BY DATE_TRUNC(day, DAY)
CLUSTER BY topic, gcp_project
AS
	SELECT DATE_TRUNC(usage_end_time, DAY) as day,
	project.name as gcp_project,
	topic,
	service.description as cost_category,
	sku.description as sku,
	invoice.month as invoice_month,
	currency,
	SUM(cost) as cost
	FROM `billing-admin-290403.billing_aggregate.aggregate`
	WHERE cost_type <> 'tax'
	GROUP BY day, gcp_project, topic, cost_category, sku, invoice_month, currency