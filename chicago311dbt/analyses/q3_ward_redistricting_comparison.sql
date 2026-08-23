with requests_in_redistricting_window as (
    select
        f.sr_number,
        d.date_year,
        w.ward_id,
        w.map_version
    from {{ ref('fct_service_requests') }} f
    join {{ ref('dim_date') }} d 
        on f.created_date_key = d.date_key
    left join {{ ref('dim_ward') }} w 
        on f.ward_map_key = w.ward_map_key
        
    where d.date_year in (2022, 2023, 2024, 2025, 2026)
),

historical_map_vol as (
    select
        ward_id,
        count(*) as vol_2015_map
    from requests_in_redistricting_window
    where map_version = '2015_MAP'
    group by ward_id
),

current_map_vol as (
    select
        ward_id,
        count(*) as vol_2023_map
    from requests_in_redistricting_window
    where map_version = '2023_MAP'
    group by ward_id
)

select
    coalesce(h.ward_id, c.ward_id) as ward_id,
    coalesce(h.vol_2015_map, 0) as historical_map_volume,
    coalesce(c.vol_2023_map, 0) as current_map_volume,
    coalesce(c.vol_2023_map, 0) - coalesce(h.vol_2015_map, 0) as volume_shift
from historical_map_vol h
full outer join current_map_vol c 
    on h.ward_id = c.ward_id
order by ward_id
