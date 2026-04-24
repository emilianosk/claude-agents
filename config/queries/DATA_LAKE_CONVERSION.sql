WITH filtered_kepler AS (
    SELECT
        ss.store,
        k.Name,
        date_trunc('HOUR', k.Date_Time) AS hour_bucket,
        k.Measures_Transactions,
        k.Measures_Inside
    FROM gold_analytics.f_kepler_hourly k
    INNER JOIN dim.selector_store ss
        ON k.Name = ss.kepler_store_name
    WHERE k.Date >= add_months(current_date(), -4)
),

non_store_bookings AS (
    SELECT
        ss.store,
        date_trunc('HOUR', sa.scheduled_start_time) AS hour_bucket,
        COUNT(DISTINCT sa.appointment_number) AS non_store_bookings
    FROM fact.salesforce_service_appointment sa
    INNER JOIN dim.selector_store ss
        ON sa.ServiceTerritoryId = ss.salesforce_service_territory_id
    WHERE sa.source IN ('Call Centre', 'Online')
      AND sa.status_category <> 'Canceled'
      AND sa.booking_date >= add_months(current_date(), -4)
    GROUP BY
        ss.store,
        date_trunc('HOUR', sa.scheduled_start_time)
),

kepler_totals AS (
    SELECT
        store,
        hour_bucket,
        SUM(Measures_Transactions) AS transactions,
        SUM(Measures_Inside) AS inside
    FROM filtered_kepler
    GROUP BY
        store,
        hour_bucket
),

final AS (
    SELECT
        k.store,
        k.hour_bucket,
        k.transactions,
        k.inside,
        COALESCE(n.non_store_bookings, 0) AS non_store_bookings,
        CASE
            WHEN k.transactions IS NOT NULL THEN
                (k.transactions - COALESCE(n.non_store_bookings, 0)) /
                NULLIF(k.inside - COALESCE(n.non_store_bookings, 0), 0)
            ELSE NULL
        END AS store_conversion_pct
    FROM kepler_totals k
    LEFT JOIN non_store_bookings n
        ON k.store = n.store
       AND k.hour_bucket = n.hour_bucket
)

SELECT *
FROM final
WHERE store_conversion_pct IS NOT NULL
ORDER BY
    store,
    hour_bucket;
