SELECT * FROM {{ ref('stg_yellow_tripdata') }}
UNION ALL
SELECT * FROM {{ ref('stg_green_tripdata') }}