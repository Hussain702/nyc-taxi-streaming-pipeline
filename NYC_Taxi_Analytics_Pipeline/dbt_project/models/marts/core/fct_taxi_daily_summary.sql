{{ config(materialized='table') }}

select
    cast(pickup_datetime as date) as trip_date,
    count(*) as total_trips,
    sum(passenger_count) as total_passengers,
    sum(total_amount) as total_revenue,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_mintues) as avg_trip_duration,
    sum(case when payment_type_id = 1 then total_amount else 0 end) as credit_card_revenue,
    sum(case when payment_type_id = 2 then total_amount else 0 end) as cash_revenue
from {{ ref('trip_enriched') }}
group by cast(pickup_datetime as date)
