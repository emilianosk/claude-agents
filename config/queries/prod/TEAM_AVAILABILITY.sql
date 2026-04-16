-- Employee-level availability by store, next 2 weeks.
SELECT
  store_id,
  employee_id,
  employee_name,
  availability_date,
  start_time,
  end_time,
  availability_status
FROM warehouse.silver_roster.team_availability
WHERE availability_date BETWEEN current_date() AND date_add(current_date(), 14)
LIMIT 5000;
