-- Published roster for current week + next 2 weeks.
SELECT
  store_id,
  employee_id,
  employee_name,
  shift_date,
  shift_start,
  shift_end,
  role_name
FROM warehouse.silver_roster.roster_forecast
WHERE shift_date BETWEEN date_sub(current_date(), 7) AND date_add(current_date(), 14)
LIMIT 5000;
