{{ config(materialized='table') }}

select * from {{ ref('reference_payment_type') }}
