{{
    config(
        materialized='view'
    )
}}
with 

fhv as (

    select * from {{ source('staging', 'fhv_external') }}

)



    select
    dispatching_base_num,
    p_ulocation_id,
    d_olocation_id,
    affiliated_base_number,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(drop_off_datetime as timestamp) as dropoff_datetime

    from fhv
    where pickup_datetime like ('2019-%')






{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}