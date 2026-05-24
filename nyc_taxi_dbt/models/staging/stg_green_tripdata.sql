SELECT
    -- identifiers
    VendorID                                            AS vendor_id,
    _source_file,

    -- timestamps
    lpep_pickup_datetime                                AS pickup_datetime,
    lpep_dropoff_datetime                               AS dropoff_datetime,

    -- trip attributes
    -- trip_type is green-only
    trip_type,
    CASE trip_type
        WHEN 1 THEN 'Street-hail'
        WHEN 2 THEN 'Dispatch'
        ELSE        'Unknown'
    END                         AS trip_type_desc,
    passenger_count,
    trip_distance,
    RatecodeID                                          AS rate_code_id,
    store_and_fwd_flag,
    PULocationID                                        AS pu_location_id,
    DOLocationID                                        AS do_location_id,
    payment_type,

    -- fare components
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    NULL                                                AS airport_fee,
    COALESCE(cbd_congestion_fee, 0)                     AS cbd_congestion_fee,
    total_amount,

    -- derived: duration
    TIMESTAMP_DIFF(
        lpep_dropoff_datetime,
        lpep_pickup_datetime,
        MINUTE
    )                                                   AS trip_duration_min,

    -- derived: reconciliation 
    {{ calculate_fare_components() }}  AS calculated_total,

    total_amount - {{ calculate_fare_components() }}  AS reporting_gap,

    CASE
        WHEN ABS(total_amount - (
            COALESCE(fare_amount, 0)
            + COALESCE(extra, 0)
            + COALESCE(mta_tax, 0)
            + COALESCE(tip_amount, 0)
            + COALESCE(tolls_amount, 0)
            + COALESCE(improvement_surcharge, 0)
            + COALESCE(congestion_surcharge, 0)
            + COALESCE(cbd_congestion_fee, 0)
        )) <= 0.01                                      THEN 'reconciled'
        
        WHEN total_amount > (
            COALESCE(fare_amount, 0)
            + COALESCE(extra, 0)
            + COALESCE(mta_tax, 0)
            + COALESCE(tip_amount, 0)
            + COALESCE(tolls_amount, 0)
            + COALESCE(improvement_surcharge, 0)
            + COALESCE(congestion_surcharge, 0)
            + COALESCE(cbd_congestion_fee, 0)
        ) THEN 'underreported_components'
        
        ELSE                                                 'overcounted_components'
    END                                                 AS reconciliation_status,

    -- data quality flags (no filtering yet — raw is sacred)
    CASE
        WHEN fare_amount <= 0       THEN TRUE
        WHEN trip_distance <= 0     THEN TRUE
        WHEN total_amount <= 0      THEN TRUE
        WHEN TIMESTAMP_DIFF(lpep_dropoff_datetime, lpep_pickup_datetime, MINUTE) <= 0 THEN TRUE
        WHEN passenger_count <= 0   THEN TRUE
        ELSE FALSE
    END                                                 AS is_invalid_trip,

    CASE payment_type
        WHEN 0 THEN 'Flex Fare'
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided'
        ELSE        'Other'
    END                                                 AS payment_type_desc,

    CASE RatecodeID
        WHEN 1 THEN 'Standard'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau/Westchester'
        WHEN 5 THEN 'Negotiated'
        WHEN 6 THEN 'Group Ride'
        ELSE        'Unknown'
    END                                                 AS rate_code_desc,

    FALSE                                               AS is_airport_trip,
    'green'                                            AS taxi_type

FROM {{ source('raw_taxi_dataset', 'raw_green_tripdata') }}