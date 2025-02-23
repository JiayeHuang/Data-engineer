{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),
grouped_data as (
    select 
        -- Revenue grouping
        service_type,
        EXTRACT(YEAR from pickup_datetime) as revenue_year,
        EXTRACT(QUARTER from pickup_datetime) as revenue_quarter,

        -- Revenue sum-up
        sum(total_amount) as revenue_quarterly_total_amount,

    from trips_data
    group by 1,2,3
)

select 
    grouped_data.service_type as service_type,
    grouped_data.revenue_year as revenue_year,
    grouped_data.revenue_quarter as revenue_quarter,
    grouped_data.revenue_quarterly_total_amount as revenue_quarterly_total_amount,
    prev_grouped_data.revenue_quarterly_total_amount as prev_revenue_quarterly_total_amount,
    grouped_data.revenue_quarterly_total_amount/prev_grouped_data.revenue_quarterly_total_amount-1 as revenue_quarterly_yoy
from grouped_data
left join grouped_data as prev_grouped_data
on grouped_data.revenue_quarter = prev_grouped_data.revenue_quarter
    and grouped_data.revenue_year = prev_grouped_data.revenue_year + 1
    and grouped_data.service_type = prev_grouped_data.service_type
where grouped_data.revenue_year = 2020 
order by 1,2,3
