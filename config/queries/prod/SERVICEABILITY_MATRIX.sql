-- Master certification matrix (update when certs change).
SELECT
  employee_id,
  employee_name,
  certification_type,
  max_services_per_shift,
  certification_status
FROM warehouse.silver_roster.serviceability_matrix
LIMIT 5000;
