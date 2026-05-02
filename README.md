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



check_bucket ──┐
               ├──► check_tables ──┐
check_dataset ─┘                   │
                                   ▼
check_bucket ──► upload_[type]_[month] ──► load_[type]_[month]



"The total amount charged to passengers. Does not include cash tips."

So it's the sum of all the itemized charge columns:
ComponentDescriptionfare_amountTime-and-distance fare from the meterextraMiscellaneous extras and surchargesmta_taxTax triggered by the metered ratetip_amountCredit card tips only (cash tips excluded)tolls_amountAll tolls paid during the tripimprovement_surchargeSurcharge assessed at flag drop (since 2015)congestion_surchargeNYS congestion surchargeairport_feeFee for pickups at LGA or JFKcbd_congestion_feeMTA Congestion Relief Zone charge (since Jan 5, 2025)
One important note for your dbt models: cash tips are excluded from total_amount. This is worth calling out in your column descriptions or tests, especially if you're building any fare analysis metrics.



⬜ Step 6 - dbt project (staging → marts → metrics)
⬜ Step 7 - Cosmos (wire dbt into Airflow)
⬜ Step 8 - GCP VM (move everything to cloud)
⬜ Step 9 - Looker Studio dashboard
⬜ Step 10 - GitHub + README (submission)


1. Negative Values in Fare Columns
The TLC documentation does not explicitly explain negative values — this is a known data quality issue in the raw dataset. Here's what the community and data practitioners have established as the causes:
Why negatives exist:
ColumnLikely cause of negativefare_amountDisputed trips, refunds, or vendor submission errorsextraCorrection entries reversing a previously recorded surchargemta_taxSame — reversal/correction recordstip_amountCredit card tip reversals or chargebackstotal_amountCascading effect — if fare is negative, total follows
Payment type code 4 means "Dispute" and code 6 means "Voided trip" — these are the most common legitimate sources of negative amounts. A disputed or voided trip gets recorded as a negative correction entry. NYC Open Data
The important insight: These are not random errors. They are real bookkeeping events — a refund is as real as a charge. The question is whether they belong in your business metric.

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


1. Payment Types — Official Definition
According to the TLC data dictionary, payment types are defined as: 0 = Flex Fare trip, 1 = Credit card, 2 = Cash, 3 = No charge, 4 = Dispute, 5 = Unknown, 6 = Voided trip. Socrata
So there IS a type 6 — you may just not have any voided trips in your January 2024 sample. Also note type 0 (Flex Fare) is new and wasn't in older versions of the dictionary — that's why some sources list only 1–5.
This is directly relevant to your negative values question — types 3, 4, and 6 are the primary sources.

2. Strategy for Negative Values — Let's Think This Through
Here's the mental model to anchor on:
What is your business metric?
You said revenue. So the question becomes: what is "revenue" for a taxi operator?
Revenue = money actually collected from passengers for completed trips.
With that definition, here's how each payment type maps:
Payment TypeNegative possible?Include in revenue?Reasoning1 — Credit cardYes (chargeback)✅ YesReal completed trip2 — CashRarely✅ YesReal completed trip3 — No chargeYes❌ NoNo money exchanged4 — DisputeYes❌ Exclude from revenueContested, unresolved5 — UnknownSometimes⚠️ Flag separatelyCan't confirm6 — VoidedYes❌ NoTrip never happened

The Right Architecture — Three Layers of Truth
This is where dbt shines. You build three perspectives on the same data:
raw layer          → everything, untouched, negatives and all
    ↓
stg layer          → cleaned, typed, flagged, no filtering yet
    ↓
mart layer         → business-purpose tables, filtered by intent
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

Why This Is the Right Answer for Infinite Lambda
This approach demonstrates exactly what a senior analytics engineer thinks about:
1. Don't destroy data — raw layer is sacred, never filtered. If someone later wants to analyze disputes, the data is there.
2. Separate concerns — revenue analytics and adjustment analytics are different questions answered by different models.
3. Explicit business logic — filtering rules live in dbt SQL with comments, not buried in a Python script or a dashboard filter nobody notices.
4. Auditability — you can always reconcile: mart_trip_revenue + mart_trip_adjustments should account for everything in raw.

Your Business Metric Statement (for the submission)
Frame it like this:

"Our primary metric is Gross Trip Revenue — defined as the sum of fare_amount for completed trips paid by credit card or cash (payment types 1 and 2), excluding disputed, voided, and zero-fare trips. Adjustments and disputes are tracked separately in mart_trip_adjustments for operational monitoring."

That one paragraph shows Infinite Lambda you understand the difference between data engineering and data governance. 


- stg_green_tripdata
- stg_yellow_tripdata


For your dbt model, rather than trying to "fix" total_amount, I'd create a calculated_total metric and flag the reconciliation gap:
sql-- in your mart model
COALESCE(fare_amount, 0)
+ COALESCE(extra, 0)
+ COALESCE(mta_tax, 0)
+ COALESCE(tip_amount, 0)
+ COALESCE(tolls_amount, 0)
+ COALESCE(improvement_surcharge, 0)
+ COALESCE(congestion_surcharge, 0)
+ COALESCE(Airport_fee, 0)
+ COALESCE(cbd_congestion_fee, 0) AS calculated_total,

total_amount - calculated_total   AS reporting_gap,

CASE
    WHEN ABS(total_amount - calculated_total) <= 0.01 THEN 'reconciled'
    WHEN total_amount > calculated_total               THEN 'underreported_components'
    ELSE                                                    'overcounted_components'
END AS reconciliation_status
This turns a data quality issue into a documented, queryable metric — which is exactly what a reviewer at Infinite Lambda would want to see.


