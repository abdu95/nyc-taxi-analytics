{#  time attributes for every pickup hour. Built from the trips themselves #}

SELECT DISTINCT
    TIMESTAMP_TRUNC(pickup_datetime, HOUR)      AS hour_timestamp,
    EXTRACT(HOUR FROM pickup_datetime)           AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM pickup_datetime)      AS day_of_week_num,
    FORMAT_TIMESTAMP('%A', pickup_datetime)      AS day_of_week_name,
    EXTRACT(MONTH FROM pickup_datetime)          AS month,
    EXTRACT(YEAR FROM pickup_datetime)           AS year,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM pickup_datetime) IN (1, 7) THEN TRUE
        ELSE FALSE
    END                                          AS is_weekend,
    
    {{ classify_time_of_day('pickup_datetime') }} AS time_slot

FROM {{ ref('int_trips_unioned') }}