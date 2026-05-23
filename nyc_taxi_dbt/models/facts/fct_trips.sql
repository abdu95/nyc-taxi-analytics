{#  grain: one row per trip. Only valid trips. All metrics pre-computed here. #}

{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge',
        partition_by={
            "field": "pickup_datetime",
            "data_type": "timestamp",
            "granularity": "month"
        }
    )
}}

WITH trips_base AS (

    SELECT
        t.*,
        pu.borough                                  AS pu_borough,
        pu.zone                                     AS pu_zone,
        do.borough                                  AS do_borough,
        do.zone                                     AS do_zone,
        dt.time_slot,
        dt.hour_of_day,
        dt.day_of_week_name,
        dt.is_weekend,

        -- deduplicate identical records (known issue: VendorID=2 submits duplicates)
        ROW_NUMBER() OVER (
            PARTITION BY
                t.pickup_datetime,
                t.dropoff_datetime,
                t.pu_location_id,
                t.do_location_id,
                t.vendor_id,
                t.taxi_type,
                CAST(t.fare_amount AS NUMERIC),   -- ← cast FLOAT64 to NUMERIC
                CAST(t.total_amount AS NUMERIC)   -- ← cast FLOAT64 to NUMERIC
            ORDER BY t.pickup_datetime
        ) AS rn

    FROM {{ ref('int_trips_unioned') }} t
    LEFT JOIN {{ ref('dim_zones') }} pu ON t.pu_location_id = pu.location_id
    LEFT JOIN {{ ref('dim_zones') }} do ON t.do_location_id = do.location_id
    LEFT JOIN {{ ref('dim_time') }} dt  ON TIMESTAMP_TRUNC(t.pickup_datetime, HOUR) = dt.hour_timestamp
    WHERE t.is_invalid_trip = FALSE
      AND t.payment_type IN (1, 2)

    {% if is_incremental() %}
      AND t.pickup_datetime > (SELECT MAX(pickup_datetime) FROM {{ this }})
    {% endif %}

)

SELECT
    -- surrogate key
    {{ dbt_utils.generate_surrogate_key([
        'pickup_datetime',
        'dropoff_datetime',
        'pu_location_id',
        'do_location_id',
        'vendor_id',
        'taxi_type',
        'fare_amount',
        'total_amount'
    ]) }}                                           AS trip_id,

    -- keys
    pickup_datetime,
    dropoff_datetime,
    pu_location_id,
    do_location_id,
    vendor_id,
    taxi_type,
    payment_type,
    payment_type_desc,
    rate_code_id,
    rate_code_desc,
    _source_file,

    -- enriched dimensions
    pu_borough,
    pu_zone,
    do_borough,
    do_zone,
    time_slot,
    hour_of_day,
    day_of_week_name,
    is_weekend,

    -- trip attributes
    passenger_count,
    trip_distance,
    trip_duration_min,
    is_airport_trip,

    -- fare components
    fare_amount,
    tip_amount,
    total_amount,
    calculated_total,
    reporting_gap,
    reconciliation_status,

    -- North Star: revenue per mile
    SAFE_DIVIDE(total_amount, trip_distance)        AS revenue_per_mile,

    -- revenue per minute
    SAFE_DIVIDE(total_amount, trip_duration_min)    AS revenue_per_min,

    -- tip rate
    SAFE_DIVIDE(tip_amount, fare_amount)            AS tip_rate,

    -- revenue per passenger
    SAFE_DIVIDE(total_amount, passenger_count)      AS revenue_per_passenger

FROM trips_base
WHERE rn = 1