SELECT
    payment_type,
    payment_type_desc,
    vendor_id,
    taxi_type,

    COUNT(*)                                        AS total_trips,

    -- tip metrics (credit card only — cash tips not recorded)
    AVG(CASE WHEN payment_type = 1 THEN tip_rate END)
                                                    AS avg_tip_rate,
    AVG(CASE WHEN payment_type = 1 THEN tip_amount END)
                                                    AS avg_tip_amount,

    -- revenue per passenger
    AVG(revenue_per_passenger)                      AS avg_revenue_per_passenger,

    -- payment split
    SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ())    AS payment_type_share,

    SUM(total_amount)                               AS gross_revenue

FROM {{ ref('fct_trips') }}
GROUP BY 1, 2, 3, 4