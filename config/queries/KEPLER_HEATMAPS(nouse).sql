-- TODO: replace with production-ready source table and filters.
-- Requirement: 6 weeks actuals, include HH:MM stamp, by store and hour.
SELECT
  store_id,
  business_date,
  hour_of_day,
  footfall,
  total_conversion_rate
FROM warehouse.silver_roster.kepler_hourly_heatmaps
WHERE business_date >= date_sub(current_date(), 42)
LIMIT 5000;
