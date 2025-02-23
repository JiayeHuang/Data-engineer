{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),
cleaned_trips_data as (
    select 
        EXTRACT(YEAR from pickup_datetime) as year,
        EXTRACT(Month from pickup_datetime) as month,
        *
    from trips_data
    where fare_amount > 0
        and trip_distance > 0
        and payment_type_description in ('Cash', 'Credit Card')
),
percent_trips_data as (
    select 
        service_type,
        PERCENTILE_CONT(fare_amount, 0.97) OVER(PARTITION BY service_type, year, month) AS fare_amout_p97,
        PERCENTILE_CONT(fare_amount, 0.95) OVER(PARTITION BY service_type, year, month) AS fare_amout_p95,
        PERCENTILE_CONT(fare_amount, 0.90) OVER(PARTITION BY service_type, year, month) AS fare_amout_p90       
    from cleaned_trips_data
    where year = 2020 and month = 4
)

select service_type,
    max(fare_amout_p97) as fare_amout_p97,
    max(fare_amout_p95) as fare_amout_p95,
    max(fare_amout_p90) as fare_amout_p90,
from percent_trips_data
group by 1