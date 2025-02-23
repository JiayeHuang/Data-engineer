{{ config(materialized='view') }}
 
with tripdata as 
(
  select *
  from {{ source('staging','fhv_trips') }}
  where dispatching_base_num is not null
)

select
   -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,    
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast(TIMESTAMP_SECONDS(CAST(pickup_datetime/1000000000 as INT64)) as timestamp) as pickup_datetime,
    cast(TIMESTAMP_SECONDS(CAST(dropOff_datetime/1000000000 as INT64)) as timestamp) as dropoff_datetime,
        
    -- vehicle info
    SAFE_CAST(dispatching_base_num as STRING) as dispatching_base_num,
    SAFE_CAST(Affiliated_base_number as STRING) as affiliated_base_num,
    coalesce({{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }},0) as sr_flag,
from tripdata

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}