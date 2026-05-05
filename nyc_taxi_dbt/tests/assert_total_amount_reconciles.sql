-- Fails if any vendor outside known non-conforming vendors
-- has a total_amount reconciliation gap.
-- Known non-conforming vendors:
--   1 = Creative Mobile (excludes congestion_surcharge from total_amount)
--   6 = Myle Technologies (null RatecodeID, malformed records)
--   7 = Helix (unitemized Newark surcharge)

SELECT *
FROM {{ ref('stg_yellow_tripdata') }}
WHERE reconciliation_status != 'reconciled'
  AND vendor_id NOT IN (1, 2, 6, 7)