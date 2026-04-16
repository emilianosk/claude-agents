-- Optional dataset: campaigns, stocktakes, events, and rollout activities.
SELECT
  store_id,
  event_date,
  event_type,
  event_name,
  expected_demand_impact,
  expected_hours_impact
FROM warehouse.silver_roster.marketing_ops_calendar
WHERE event_date BETWEEN current_date() AND date_add(current_date(), 14)
LIMIT 2000;
