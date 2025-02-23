{{ config(materialized='table') }}

with trips_data as (
    select *,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration,
    from {{ ref('dim_fhv_trips') }}
),
percent_trips_data as (
    select 
        *,
        row_number() over(partition by pickup_year, pickup_month, pickup_locationid, dropoff_locationid) as rn,
        PERCENTILE_CONT(trip_duration, 0.90) OVER(PARTITION BY pickup_year, pickup_month, pickup_locationid, dropoff_locationid) AS trip_duration_p90,    
    from trips_data
)

select pickup_year,
    pickup_month,
    pickup_zone,
    dropoff_zone,
    trip_duration,
    trip_duration_p90
from percent_trips_data
where rn = 1
    and pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
    and pickup_year = 2019 and pickup_month = 11
order by pickup_zone, trip_duration_p90 DESC