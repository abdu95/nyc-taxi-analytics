SELECT
    day_of_week_name,
    hour_of_day,
    time_slot,
    is_weekend,
    taxi_type,

    COUNT(*)                                        AS total_trips,
    SUM(total_amount)                               AS gross_revenue,
    AVG(trip_duration_min)                          AS avg_trip_duration_min,
    AVG(revenue_per_mile)                           AS avg_revenue_per_mile,
    EXTRACT(DAYOFWEEK FROM pickup_datetime)     AS day_of_week_num,
    -- busiest combo flag
    RANK() OVER (
        ORDER BY COUNT(*) DESC
    )                                               AS demand_rank

FROM {{ ref('fct_trips') }}
GROUP BY 1, 2, 3, 4, 5