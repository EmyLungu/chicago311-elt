with requests_in_redistricting_window as (
    select
        f.sr_number,
        d.date_year,
        w_hist.ward_id as historical_ward_id,
        w_curr.ward_id as current_ward_id
    from {{ ref('fct_service_requests') }} f
    join {{ ref('dim_date') }} d 
        on f.created_date_key = d.date_key
    
    left join {{ ref('dim_ward') }} w_hist 
        on f.ward_map_key = w_hist.ward_map_key
        
    left join {{ ref('dim_ward') }} w_curr 
        on w_hist.ward_id = w_curr.ward_id 
       and w_curr.is_current = true
       
    where d.date_year in (2022, 2023, 2024, 2025, 2026)
),

historical_aggregated as (
    select
        historical_ward_id as ward_id,
        count(*) as historical_request_volume
    from requests_in_redistricting_window
    where historical_ward_id is not null
    group by historical_ward_id
),

current_aggregated as (
    select
        current_ward_id as ward_id,
        count(*) as current_request_volume
    from requests_in_redistricting_window
    where current_ward_id is not null
    group by current_ward_id
)

select
    coalesce(h.ward_id, c.ward_id) as ward_id,
    coalesce(h.historical_request_volume, 0) as historical_volume,
    coalesce(c.current_request_volume, 0) as current_volume,
    coalesce(h.historical_request_volume, 0) - coalesce(c.current_request_volume, 0) as volume_difference,
    round(
        (coalesce(h.historical_request_volume, 0) - coalesce(c.current_request_volume, 0)) * 100.0 / 
        nullif(coalesce(c.current_request_volume, 0), 0), 2
    ) as percentage_shift
from historical_aggregated h
full outer join current_aggregated c 
    on h.ward_id = c.ward_id
order by abs(volume_difference) desc
