-- NPS trend by store, last 6 weeks.
SELECT
  store_id,
  week_start,
  nps_score,
  nps_responses,
  key_theme
FROM warehouse.silver_roster.nps_weekly
WHERE week_start >= date_sub(current_date(), 42)
LIMIT 2000;
