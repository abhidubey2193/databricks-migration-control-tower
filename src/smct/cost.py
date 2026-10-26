"""Billing query example; returned DBU quantities are not dollar savings.
Billing tables may lag and shared clusters cannot always be attributed.
"""
BILLING_SQL = """
SELECT usage_metadata.job_id AS job_id, billing_origin_product,
       sku_name, usage_unit, SUM(usage_quantity) AS usage_quantity
FROM system.billing.usage
WHERE usage_start_time >= current_timestamp() - INTERVAL 30 DAYS
  AND usage_unit = 'DBU'
GROUP BY usage_metadata.job_id, billing_origin_product, sku_name, usage_unit
"""
