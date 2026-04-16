-- Primary decision metric. Walk-in conversion only (exclude pre-booked piercing).
SELECT
  store_id,
  business_date,
  hour_of_day,
  footfall,
  walk_in_transactions,
  walk_in_conversion_rate
FROM warehouse.silver_roster.walkin_conversion_hourly
WHERE business_date >= date_sub(current_date(), 42)
LIMIT 5000;
