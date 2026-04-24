SELECT * FROM gold_analytics.f_kepler_hourly 
WHERE Measures_Inside IS NOT NULL
AND `Date` >= add_months(current_date(), -4) AND Measures_Dwell_Time <> '0 mins 0 secs'
ORDER BY name, Date_Time desc;