SELECT
    pu_zone,
    pu_borough,
    time_slot,
    day_of_week_name,
    is_weekend,
    taxi_type,

    COUNT(*)                                        AS total_trips,
    SUM(total_amount)                               AS gross_revenue,
    AVG(revenue_per_mile)                           AS avg_revenue_per_mile,
    AVG(trip_distance)                              AS avg_trip_distance,
    AVG(trip_duration_min)                          AS avg_trip_duration_min

FROM {{ ref('fct_trips') }}
GROUP BY 1, 2, 3, 4, 5, 6