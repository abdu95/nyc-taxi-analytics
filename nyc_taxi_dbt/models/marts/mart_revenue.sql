-- Reconciliation gap documented: known non-conforming vendors 1, 2, 6, 7
SELECT
    pu_borough,
    rate_code_desc,
    EXTRACT(YEAR FROM pickup_datetime)              AS year,
    EXTRACT(MONTH FROM pickup_datetime)             AS month,
    taxi_type,

    -- volume
    COUNT(*)                                        AS total_trips,

    -- gross revenue
    SUM(total_amount)                               AS gross_revenue,
    AVG(total_amount)                               AS avg_fare,

    -- airport premium
    SUM(CASE WHEN is_airport_trip THEN total_amount ELSE 0 END)
                                                    AS airport_revenue,
    AVG(CASE WHEN is_airport_trip THEN total_amount END)
                                                    AS avg_airport_fare,
    AVG(CASE WHEN NOT is_airport_trip THEN total_amount END)
                                                    AS avg_standard_fare,

    -- revenue per mile
    AVG(revenue_per_mile)                           AS avg_revenue_per_mile,

    -- trip share
    COUNTIF(is_airport_trip) / COUNT(*)             AS airport_trip_share

FROM {{ ref('fct_trips') }}
GROUP BY 1, 2, 3, 4, 5