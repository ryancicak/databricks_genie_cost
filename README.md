# databricks_genie_cost
Run the following SQL for either a shared warehouse or dedicated warehouse (to Genie), and the query will estimate costs.

- Replace GenieWarehouse > 4b9b953939869799 with your warehouse id
- Replace GenieQueryDurations > query_source.genie_space_id 01f04c717eea15828cc19d0697909497 with your genie space id (room id)
- Remember, Genie is a SaaS. These costs are generated from Genie changing Natural Language to DBSQL - meaning you pay for executing DBSQL.
  
```
WITH GenieWarehouse AS (
  SELECT '4b9b953939869799' AS warehouse_id 
),
GenieQueryDurations AS (
  SELECT
    DATE(start_time) AS query_date,
    SUM(total_task_duration_ms / 1000.0) AS genie_total_task_seconds
  FROM system.query.history
  WHERE compute.warehouse_id = (SELECT warehouse_id FROM GenieWarehouse)
    AND query_source.genie_space_id = '01f04c717eea15828cc19d0697909497'
    AND start_time >= DATE_SUB(CURRENT_DATE(), 60)
  GROUP BY DATE(start_time)
),
WarehouseQueryDurations AS (
  SELECT
    DATE(start_time) AS query_date,
    SUM(total_task_duration_ms / 1000.0) AS warehouse_total_task_seconds
  FROM system.query.history
  WHERE compute.warehouse_id = (SELECT warehouse_id FROM GenieWarehouse)
    AND start_time >= DATE_SUB(CURRENT_DATE(), 60)
  GROUP BY DATE(start_time)
),
WarehouseBilling AS (
  SELECT
    u.usage_date,
    SUM(u.usage_quantity) AS total_dbus,
    MAX(p.pricing.effective_list.default) AS dbu_price_usd,
    SUM(u.usage_quantity) * MAX(p.pricing.effective_list.default) AS total_cost_usd
  FROM system.billing.usage u
  INNER JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
  WHERE u.usage_metadata.warehouse_id = (SELECT warehouse_id FROM GenieWarehouse)
    AND u.usage_date >= DATE_SUB(CURRENT_DATE(), 60)
  GROUP BY u.usage_date
),
AttributedCosts AS (
  SELECT
    COALESCE(wq.query_date, wb.usage_date) AS date,
    COALESCE(gq.genie_total_task_seconds, 0) AS genie_total_task_seconds,
    COALESCE(wq.warehouse_total_task_seconds, 0) AS warehouse_total_task_seconds,
    CASE
      WHEN COALESCE(wq.warehouse_total_task_seconds, 0) > 0
      THEN COALESCE(gq.genie_total_task_seconds, 0) / wq.warehouse_total_task_seconds
      ELSE 0
    END AS genie_task_ratio,
    COALESCE(wb.total_dbus, 0) AS warehouse_total_dbus,
    COALESCE(wb.dbu_price_usd, 0) AS dbu_price_usd,
    COALESCE(wb.total_cost_usd, 0) AS warehouse_total_cost_usd,
    CASE
      WHEN COALESCE(wq.warehouse_total_task_seconds, 0) > 0
      THEN (COALESCE(gq.genie_total_task_seconds, 0) / wq.warehouse_total_task_seconds) * COALESCE(wb.total_cost_usd, 0)
      ELSE 0
    END AS genie_full_cost_usd  -- Full attributed cost (including idle)
  FROM WarehouseQueryDurations wq
  FULL OUTER JOIN WarehouseBilling wb ON wq.query_date = wb.usage_date
  LEFT JOIN GenieQueryDurations gq ON COALESCE(wq.query_date, wb.usage_date) = gq.query_date
)
SELECT
  date,
  genie_total_task_seconds,
  genie_task_ratio,
  genie_full_cost_usd
FROM AttributedCosts
WHERE genie_total_task_seconds > 0
UNION ALL
SELECT
  'Total' AS date,
  SUM(genie_total_task_seconds) AS genie_total_task_seconds,
  NULL AS genie_task_ratio,  
  SUM(genie_full_cost_usd) AS genie_full_cost_usd
FROM AttributedCosts
WHERE genie_total_task_seconds > 0
ORDER BY date DESC;
```
