with 

source as (

    select * from {{ source('staging', 'fhv_trips_2019') }}

),

renamed as (

    select
        dispatching_base_num,
        cast(pickup_datetime as DATETIME) as pickup_datetime,
        cast(dropoff_datetime as DATETIME) as dropoff_datetime,
        cast(pulocationid as INT64) as pulocationid,
        cast(dolocationid as INT64) as dolocationid,
        sr_flag,
        affiliated_base_number

    from source

)

select * from renamed
