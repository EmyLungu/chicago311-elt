{{
    config(
        materialized='incremental',
        unique_key='sr_number',
        incremental_strategy='merge'
    )
}}

select
    -- Primary Key
    r.sr_number,
    r.parent_sr_number,
    r.legacy_sr_number,

    -- Foreign Keys
    w.ward_map_key,         -- dim_ward
    st.service_type_key,    -- dim_service_type
    g.location_key,         -- dim_location
    cast(strftime(r.created_date, '%Y%m%d') as integer) as created_date_key, -- dim_date
    cast(strftime(r.closed_date, '%Y%m%d') as integer) as closed_date_key,   -- dim_date

    r.status,
    r.origin,
    r.is_duplicate,
    r.is_legacy_record,

    -- Location
    r.street_address,
    r.street_number,
    r.street_direction,
    r.street_name,
    r.street_type,
    r.city,
    r.state,
    r.latitude,
    r.longitude,
    r.x_coordinate,
    r.y_coordinate,

    -- Timestamps
    r.created_date,
    r.last_modified_date,
    r.closed_date,

    -- Metrics
    date_diff('hour', r.created_date, r.closed_date) as resolution_time_hours,
    date_diff('hour', r.created_date, r.closed_date) / 24.0 as resolution_time_days
from {{ ref('stg_chicago311') }} r

left join {{ ref('dim_ward') }} w
    on r.ward_id = w.ward_id
   and r.created_date >= w.effective_from
   and r.created_date <= w.effective_to
left join {{ ref('dim_service_type') }} st
    on coalesce(r.sr_type, '') = coalesce(st.sr_type, '')
   and coalesce(r.sr_short_code, '') = coalesce(st.sr_short_code, '')
left join {{ ref('dim_location') }} g
    on coalesce(r.community_area_id, -1) = coalesce(g.community_area_id, -1)
   and coalesce(r.zip_code, '') = coalesce(g.zip_code, '')
   and coalesce(r.police_beat, '') = coalesce(g.police_beat, '')

{% if is_incremental() %}
    where coalesce(last_modified_date, created_date) >= (
        select max(coalesce(last_modified_date, created_date)) from {{ this }}
    )
{% endif %}
