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



The dataset is public TLC data so there's no real PII — but the most defensible choice for the interview is hashing, specifically on vendor_id.
Here's the business justification: in a real production context, vendor_id maps to contractual commercial relationships between TLC and vendors (VeriFone, Creative Mobile, Helix). Revenue performance by vendor is commercially sensitive — you wouldn't want all analysts to see which vendor processes the most revenue or has the worst data quality. You'd hash it in marts exposed to wider audiences, keeping the raw mapping only in the raw/staging layer.


This is actually what you've already built architecturally — the marts deliberately aggregate away from raw location IDs. You just need to make the intent explicit by adding policy tags in BigQuery and documenting it.
That's a real data governance story: "We deliberately don't expose raw location IDs in marts because timestamp + precise zone = potential re-identification risk. Borough-level aggregation in the mart layer is intentional."


"This is public data with no PII. However, I've implemented the architectural equivalent of access control — raw zone IDs and timestamps are only available in staging, marts deliberately expose borough-level aggregations. In a production context with real transaction IDs or driver IDs, I would implement hashing at the staging layer."


Back to PULocationID / DOLocationID — this is the real one. And the masking approach is actually already implemented in our architecture:

Raw/staging layer → exact zone IDs (pu_location_id, do_location_id)
Marts → borough level only (pu_borough, do_borough)

That is masking. We're reducing precision deliberately.
The only thing missing is making the intent explicit in dbt via column descriptions:
yaml- name: pu_location_id
  description: >
    Raw pickup zone ID. Masked to borough level in mart layer
    to prevent re-identification of individual passenger journeys
    via location + timestamp combination.
And in marts:
yaml- name: pu_borough
  description: >
    Pickup borough — masked aggregation of pu_location_id.
    Deliberately exposed at borough level only to protect passenger privacy.
Interview story:
"Location IDs combined with timestamps create re-identification risk. I mask exact zone IDs to borough level in the mart layer. This is enforced architecturally — analysts only have access to mart models which never expose raw zone IDs."




📊 All Extractable Business Metrics
💰 Revenue Metrics (Primary)
MetricFormulaBusiness ValueRevenue per Mile ⭐total_amount / trip_distanceCore efficiency KPIRevenue per Minutetotal_amount / trip_duration_minTime efficiencyGross Revenue by ZoneSUM(total_amount) GROUP BY PULocationIDDemand heatmapAirport Revenue PremiumAvg total_amount where airport_fee > 0 vs standardPricing strategyRevenue by Rate TypeSUM(total_amount) GROUP BY RatecodeIDJFK vs standard mix
🚗 Operational Metrics
MetricFormulaBusiness ValueDriver Utilization (proxy) ⭐Trips per hour per zoneSupply/demand balanceAvg Trip Durationtpep_dropoff_datetime - tpep_pickup_datetimeOperational planningTrips per Time SlotCOUNT(*) GROUP BY HOUR(pickup)Peak hours detectionRush Hour vs Off-Peak Volumeextra > 0 flag analysisSurge pricing impact
👤 Customer Metrics
MetricFormulaBusiness ValueTip Ratetip_amount / fare_amountSatisfaction proxyRevenue per Passengertotal_amount / passenger_countYield per seatCash vs Card SplitCOUNT(*) GROUP BY payment_typePayment behaviorGroup Ride ShareTrips where RatecodeID = 6Pooling opportunity
📍 Geographic Metrics
MetricFormulaBusiness ValueTop Pickup ZonesCOUNT(*) GROUP BY PULocationIDHotspot analysisAvg Fare by BoroughJoin zone lookup → aggregateGeographic pricingAirport Trip Shareairport_fee > 0 as % of total trips~8% of yellow taxi rides have an airport fee Rowzero — baseline to beat

🎯 Recommended North Star Metric for the Task
Revenue per Mile by Zone & Time of Day — because it combines:

A single quantifiable number ✅
Geographic dimension (zones → boroughs) ✅
Temporal dimension (rush hour vs off-peak) ✅
Direct business decision: where and when should drivers operate?


🗂️ Suggested dbt Model Structure
raw.tlc_trips
    ↓
stg_tlc__trips          -- clean types, derived duration, trip_revenue
stg_tlc__zones          -- zone → borough lookup (joinable)
    ↓
fct_trips               -- grain: 1 row per trip
dim_zones               -- PULocationID/DOLocationID enrichment
dim_time                -- hour, day_of_week, is_rush_hour
    ↓
mart_driver_performance -- revenue_per_mile, utilization_rate
mart_zone_demand        -- trips/revenue by zone & time slot



✅ Final Metric Set
🌟 North Star

Revenue per Mile — by Zone & Time of Day

💰 Revenue

Gross Revenue by Zone / Borough
Airport Revenue Premium
Revenue by Rate Type (JFK vs Standard vs Newark)

🚗 Operational

Avg Trip Duration ⭐ (new)
Driver Utilization (trips per hour per zone)
Rush Hour vs Off-Peak Volume

📅 Demand Patterns

Busiest Days of Week ⭐ (new) — COUNT(*) GROUP BY DAY_OF_WEEK(pickup)
Trips by Hour of Day
Peak Zone by Day/Hour combo

👤 Customer

Tip Rate
Revenue per Passenger
Cash vs Card Split


🗺️ How Trip Duration & Busiest Days Strengthen the Story
Trip Duration  →  reveals where drivers LOSE time (long trips, low $/mile)
Busiest Days   →  tells operations WHEN to deploy more supply
Combined       →  "Thursday rush hour, Midtown → JFK = highest $/mile + longest duration"
                   = the single best shift for a driver
This gives you a complete operations intelligence story: where, when, and how long — all feeding into the north star of Revenue per Mile.