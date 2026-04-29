payment types are defined as: 
0 = Flex Fare trip, 1 = Credit card, 2 = Cash, 3 = No charge, 4 = Dispute, 5 = Unknown, 6 = Voided trip. 
Include in revenue: 1 and 2


Strategy — handle in dbt, not in ingestion:
This is the key architectural decision. Your raw layer should preserve everything as-is — negatives included. You handle data quality in dbt staging models. This is the correct modern data engineering approach:
raw layer (BigQuery)     → keep all rows including negatives
    ↓ dbt staging        → document, flag, filter negatives
    ↓ dbt marts          → only valid trips for business metrics
    ↓ Looker Studio      → clean numbers
In your dbt staging model you will:
sql-- stg_yellow_taxi.sql
SELECT
    *,
    -- flag suspicious records
    CASE
        WHEN fare_amount <= 0     THEN TRUE
        WHEN trip_distance <= 0   THEN TRUE
        WHEN total_amount <= 0    THEN TRUE
        ELSE FALSE
    END AS is_invalid_trip

FROM {{ source('raw_taxi_dataset', 'yellow_tripdata_2024_01') }}
Then in your mart model you simply filter:
sql-- mart_trip_revenue.sql
SELECT * FROM {{ ref('stg_yellow_taxi') }}
WHERE is_invalid_trip = FALSE
This way:

Raw data is untouched ✅
You have full auditability (can always see how many were filtered) ✅
Business metrics are clean ✅
Infinite Lambda can see you understand the separation of concerns ✅



In your mart layer you build two separate models:
mart_trip_revenue.sql — operational revenue:
sql-- Only real, completed, paid trips
SELECT *
FROM {{ ref('stg_yellow_taxi') }}
WHERE payment_type IN (1, 2)   -- credit card and cash only
  AND fare_amount > 0
  AND trip_distance > 0
mart_trip_adjustments.sql — disputes, voids, refunds:
sql-- Separate view of financial adjustments
SELECT
    payment_type,
    CASE payment_type
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 6 THEN 'Voided'
    END                          AS adjustment_type,
    COUNT(*)                     AS trip_count,
    SUM(fare_amount)             AS total_adjustment_amount,
    SUM(total_amount)            AS total_amount_adjusted
FROM {{ ref('stg_yellow_taxi') }}
WHERE payment_type IN (3, 4, 6)
   OR fare_amount <= 0
GROUP BY 1, 2

