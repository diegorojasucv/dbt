{{
  config(
    materialized = 'incremental'
  )
}}

with sales as (
    
    select 
        *,
        current_timestamp as current_timestamp_at
    from {{ source('databricks_source', 'int_sales') }}
)

select *
from sales
{% if is_incremental() %}

where current_timestamp_at > (select max(current_timestamp_at) from {{ this }})

{% endif %}