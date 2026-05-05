SELECT
    pu_zone,
    pu_borough,
    hour_of_day,
    time_slot,
    is_weekend,
    taxi_type,

    COUNT(*)                                        AS total_trips,
    AVG(trip_duration_min)                          AS avg_trip_duration_min,
    AVG(revenue_per_mile)                           AS avg_revenue_per_mile,
    AVG(revenue_per_min)                            AS avg_revenue_per_min,
    SUM(total_amount)                               AS gross_revenue,

    -- trips per hour proxy: revenue density
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT hour_of_day))
                                                    AS trips_per_hour

FROM {{ ref('fct_trips') }}
GROUP BY 1, 2, 3, 4, 5, 6