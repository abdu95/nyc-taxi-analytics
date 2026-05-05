-- fct_trips should contain no invalid trips
-- since we filter is_invalid_trip = FALSE in the model.
-- This test catches if that filter ever gets accidentally removed.

SELECT *
FROM {{ ref('fct_trips') }}
WHERE total_amount <= 0
   OR trip_distance <= 0
   OR trip_duration_min <= 0