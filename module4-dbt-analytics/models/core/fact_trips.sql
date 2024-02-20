{{ config(materialized="table") }}

with
    green_table as (
        select *, 'Green' as service_type from {{ ref("stg_green_tripdata") }}
    ),
    yellow_table as (
        select *, 'Yellow' as service_type from {{ ref("stg_yellow_tripdata") }}
    ),
    trips_unioned as (
        select *
        from green_table
        union ALL
        select * from yellow_table
    ),
    zones_table as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')

select *
from trips_unioned
inner join
    zones_table as pickupzones on trips_unioned.pickup_locationid = pickupzones.locationid
inner join
    zones_table as dropoffzones on trips_unioned.pickup_locationid = dropoffzones.locationid
