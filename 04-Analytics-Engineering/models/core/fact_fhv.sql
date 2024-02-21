{{
    config(
        materialized='table'
    )
}}

with stg_fhv as (
    select * from {{ ref('stg_fhv_exteral') }}

),
dim_zones as(
    select * from {{ ref('dim_zones') }}
    where borough !='Unknown'
)

select 
stg_fhv.dispatching_base_num,
stg_fhv.p_ulocation_id,
pick.borough as pickup_borough,
pick.zone as pickup_zone,
stg_fhv.d_olocation_id,
dropo.borough as dropoff_borough,
dropo.zone as dropoff_zone,
stg_fhv.affiliated_base_number,
stg_fhv.pickup_datetime,
stg_fhv.dropoff_datetime




 from stg_fhv
inner join dim_zones as pick 
on stg_fhv.p_ulocation_id = pick.locationid
inner join dim_zones as dropo
on stg_fhv.d_olocation_id =dropo.locationid