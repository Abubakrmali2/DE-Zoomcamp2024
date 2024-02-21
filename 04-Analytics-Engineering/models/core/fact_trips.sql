{{
    config(
        materialized='table'
    )
}}
with all_greent_taxi_external as (
    select * ,
        'Green' as Service_type
    from {{ ref('stg_all_greent_taxi_external') }}
),
all_yellow_taxi_external as (
    select * ,
        'Yellow' as Service_type
    from {{ ref('stg_all_yellow_taxi_external') }}

),
trips_unioned as (
    select * from all_greent_taxi_external
    union all 
    select * from all_yellow_taxi_external
),

dim_zones as(
    select * from {{ ref('dim_zones') }}
    where borough !='Unknown'
)

select trips_unioned.tripid, 
    trips_unioned.vendor_id, 
    trips_unioned.service_type,
    trips_unioned.ratecode_id, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
     from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid=pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid=dropoff_zone.locationid

-- to run all dbt build --select +fact_trips+ --vars '{'is_test_run': 'false'}'