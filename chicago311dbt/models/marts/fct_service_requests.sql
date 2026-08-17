{{
    config(
        materialized='incremental',
        unique_key='sr_number',
        incremental_strategy='merge'
    )
}}

select
    r.*,
    w.map_version as ward_map_version,
    w.ward_map_key
from {{ ref('stg_chicago311') }} r
left join {{ ref('dim_ward') }} w
    on r.ward_id = w.ward_id
   and r.created_date >= w.effective_from
   and r.created_date <= w.effective_to

{% if is_incremental() %}
    where coalesce(last_modified_date, created_date) >= (
        select max(coalesce(last_modified_date, created_date)) from {{ this }}
    )
{% endif %}
