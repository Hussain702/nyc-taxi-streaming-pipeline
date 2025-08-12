  
{{config(materialized='view')}}

SELECT 
    cast( VendorID  as int) as vendor_id,
    cast(tpep_pickup_datetime as datetime2) as pickup_datetime,
    cast(tpep_dropoff_datetime as datetime2) as dropoff_datetime,
    cast(passenger_count as int) as passenger_count,
    trip_distance,
    cast(ratecodeid as int) as rate_code_id,
    store_and_fwd_flag,
    cast(pulocationid as int) as pickup_location_id,
    cast(dolocationid as int) as dropoff_location_id,
    cast(payment_type as int) as payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
from {{ source('staging', 'Staging_ny_taxi') }}
