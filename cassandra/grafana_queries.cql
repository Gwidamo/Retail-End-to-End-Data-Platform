-- grafana panels queries
-- Dashboard 1: Real-Time Revenue & Trends
-- Query 1: Total Revenue (Filtered by country variable $country)
SELECT sum(total_sales_usd) as "Revenue"
FROM sales_items_realtime
WHERE event_ts >= $__from 
AND event_ts <= $__to 
AND store_country IN (${country:singlequote})
AND is_cancel = 0
AND is_return = 0
ALLOW FILTERING

-- Query 2: Top Selling Products (Filtered by country variable $country)
SELECT product_name, quantity
FROM sales_items_realtime
WHERE event_ts >= $__from 
AND event_ts <= $__to 
AND store_country IN (${country:singlequote})
AND is_cancel = 0
AND is_return = 0
ALLOW FILTERING


-- Query 3: Revenue Over Time trend (Filtered by country variable $country)
SELECT event_ts, total_sales_usd
FROM sales_items_realtime
WHERE event_ts >= $__from 
AND event_ts <= $__to 
AND store_country IN (${country:singlequote})
AND is_cancel = 0
AND is_return = 0
ALLOW FILTERING