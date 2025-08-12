{{ config(materialized='table') }}

select
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.payment_type_id,
    p.payment_type_desc,
    t.total_amount,
    t.tip_amount,
    t.tolls_amount,
    t.trip_duration_mintues,
    t.revnue_per_mile
from {{ ref('trip_enriched') }} t
left join {{ ref('dim_payment_type') }} p
    on t.payment_type_id = p.payment_type_id

