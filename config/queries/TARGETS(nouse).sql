-- Weekly targets by store.
SELECT
  store_id,
  week_start,
  revenue_target,
  roster_hours_target,
  revenue_per_hour_target
FROM warehouse.silver_roster.store_targets
WHERE week_start BETWEEN date_sub(current_date(), 7) AND date_add(current_date(), 14)
LIMIT 1000;
