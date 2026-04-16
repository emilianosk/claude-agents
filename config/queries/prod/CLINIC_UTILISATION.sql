-- Deputy bookings / data lake hourly clinic utilisation.
SELECT
  store_id,
  business_date,
  hour_of_day,
  booked_slots,
  available_slots,
  utilisation_pct
FROM warehouse.silver_roster.clinic_utilisation_hourly
WHERE business_date >= date_sub(current_date(), 42)
LIMIT 5000;
