
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

SELECT

    -- surrogate key
    {{ dbt_utils.generate_surrogate_key([
        'pickup_datetime',
        'pu_location_id',
        'do_location_id',
        'vendor_id',
        'taxi_type'
    ]) }}                                           AS trip_id,

    -- keys
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pu_location_id,
    t.do_location_id,
    t.vendor_id,
    t.taxi_type,
    t.payment_type,
    t.payment_type_desc,
    t.rate_code_id,
    t.rate_code_desc,
    t._source_file,

    -- enriched dimensions
    pu.borough                                  AS pu_borough,
    pu.zone                                     AS pu_zone,
    do.borough                                  AS do_borough,
    do.zone                                     AS do_zone,
    dt.time_slot,
    dt.hour_of_day,
    dt.day_of_week_name,
    dt.is_weekend,

    -- trip attributes
    t.passenger_count,
    t.trip_distance,
    t.trip_duration_min,
    t.is_airport_trip,

    -- fare components
    t.fare_amount,
    t.tip_amount,
    t.total_amount,
    t.calculated_total,
    t.reporting_gap,
    t.reconciliation_status,

    -- North Star: revenue per mile
    SAFE_DIVIDE(t.total_amount, t.trip_distance)            AS revenue_per_mile,

    -- revenue per minute
    SAFE_DIVIDE(t.total_amount, t.trip_duration_min)        AS revenue_per_min,

    -- tip rate
    SAFE_DIVIDE(t.tip_amount, t.fare_amount)                AS tip_rate,

    -- revenue per passenger
    SAFE_DIVIDE(t.total_amount, t.passenger_count)          AS revenue_per_passenger

FROM {{ ref('int_trips_unioned') }} t
LEFT JOIN {{ ref('dim_zones') }} pu ON t.pu_location_id = pu.location_id
LEFT JOIN {{ ref('dim_zones') }} do ON t.do_location_id = do.location_id
LEFT JOIN {{ ref('dim_time') }} dt  ON TIMESTAMP_TRUNC(t.pickup_datetime, HOUR) = dt.hour_timestamp
WHERE t.is_invalid_trip = FALSE
  AND t.payment_type IN (1, 2)   -- credit card and cash only for revenue


{% if is_incremental() %}
  AND t.pickup_datetime > (SELECT MAX(pickup_datetime) FROM {{ this }})
{% endif %}