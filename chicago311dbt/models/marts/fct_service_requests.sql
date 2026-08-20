{{
    config(
        materialized='incremental',
        unique_key='sr_number',
        incremental_strategy='merge'
    )
}}

with dim_location_dedup as (
    select
        coalesce(community_area_id, -1) as community_area_id,
        coalesce(zip_code, '') as zip_code,
        coalesce(police_beat, '') as police_beat,
        min(location_key) as location_key
    from {{ ref('dim_location') }}
    group by 1, 2, 3
),

dim_service_type_dedup as (
    select
        coalesce(sr_type, '') as sr_type,
        coalesce(sr_short_code, '') as sr_short_code,
        min(service_type_key) as service_type_key
    from {{ ref('dim_service_type') }}
    group by 1, 2
)

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
   and (r.created_date <= w.effective_to or w.effective_to is null)

left join dim_service_type_dedup st
    on coalesce(r.sr_type, '') = st.sr_type
   and coalesce(r.sr_short_code, '') = st.sr_short_code

left join dim_location_dedup g
    on coalesce(r.community_area_id, -1) = g.community_area_id
   and coalesce(r.zip_code, '') = g.zip_code
   and coalesce(r.police_beat, '') = g.police_beat

{% if is_incremental() %}
    where coalesce(last_modified_date, created_date) >= (
        select max(coalesce(last_modified_date, created_date)) from {{ this }}
    )
{% endif %}
