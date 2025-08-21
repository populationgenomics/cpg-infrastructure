CREATE OR REPLACE MATERIALIZED VIEW `billing-admin-290403.billing_aggregate.aggregate_monthly_cost`
AS
	SELECT topic, (CASE
	  WHEN service.description='Cloud Storage' THEN 'Storage Cost'
	  ELSE 'Compute Cost'
	END) as cost_category, invoice.month, SUM(cost) as cost FROM `billing-admin-290403.billing_aggregate.aggregate`
	WHERE cost_type <> 'tax'
	GROUP BY topic, cost_category, invoice.month
