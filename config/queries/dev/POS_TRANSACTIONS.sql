-- Must include piercing + fashion split by hour.
SELECT
  store_id,
  business_date,
  hour_of_day,
  piercing_transactions,
  fashion_transactions,
  piercing_revenue,
  fashion_revenue,
  total_revenue
FROM warehouse.silver_roster.pos_transactions_hourly
WHERE business_date >= date_sub(current_date(), 42)
LIMIT 5000;
