{{config(materialized='view')}}

SELECT * ,
abs(datediff(minute,pickup_datetime,dropoff_datetime)) as trip_duration_mintues,
case
    when trip_distance=0 then NULL
    else total_amount/trip_distance
  end as revnue_per_mile
from {{ref('stg_ny_taxi')}}    