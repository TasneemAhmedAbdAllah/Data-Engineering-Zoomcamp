{{ config(materialized="table") }}

with
    fhv_trips as (select * from {{ ref("stg_fhv_trips_2019") }}),

    zones_table as (select * from {{ ref("dim_zones") }} where borough != 'Unknown'),

    fact_fhv as (
        select
            fhv_trips.dispatching_base_num,
            fhv_trips.pickup_datetime,
            fhv_trips.dropoff_datetime,
            fhv_trips.pulocationid,
            pickup_zones.borough as pickup_borough,
            pickup_zones.zone as pickup_zone,
            pickup_zones.service_zone as pickup_service_zone,
            fhv_trips.dolocationid,
            dropoff_zones.borough as dropoff_borough,
            dropoff_zones.zone as dropoff_zone,
            dropoff_zones.service_zone as dropoff_service_zone,
            fhv_trips.sr_flag,
            fhv_trips.affiliated_base_number
        from fhv_trips
        inner join
            zones_table pickup_zones on fhv_trips.pulocationid = pickup_zones.locationid
        inner join
            zones_table dropoff_zones
            on fhv_trips.dolocationid = dropoff_zones.locationid
    )

select *
from fact_fhv
