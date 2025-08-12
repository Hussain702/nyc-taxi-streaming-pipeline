{% snapshot payment_type_snapshot %}
{{ config(
    target_schema='snapshots',
    unique_key='payment_type_id',
    strategy='check',
    check_cols=['payment_type_desc']
) }}
select * from {{ ref('dim_payment_type') }}
{% endsnapshot %}
