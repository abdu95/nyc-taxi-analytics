-- Green taxis cannot pick up at airports.
-- is_airport_trip should always be FALSE for green.

SELECT *
FROM {{ ref('stg_green_tripdata') }}
WHERE is_airport_trip = TRUE