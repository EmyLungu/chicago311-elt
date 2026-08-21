with pothole_graffiti_requests as (
    select
        g.community_area_id,
        d.date_year,
        f.resolution_time_days
    from {{ ref('fct_service_requests') }} f
    join {{ ref('dim_location') }} g 
        on f.location_key = g.location_key
    join {{ ref('dim_service_type') }} s 
        on f.service_type_key = s.service_type_key
    join {{ ref('dim_date') }} d 
        on f.created_date_key = d.date_key
    where 
        f.status = 'COMPLETED'
        and f.closed_date is not null
        and (
            lower(s.sr_type) like '%pothole%' 
            or lower(s.sr_type) like '%graffiti%'
        )
),

yearly_medians as (
    select
        community_area_id,
        date_year,
        median(resolution_time_days) as median_resolution_days
    from pothole_graffiti_requests
    where community_area_id is not null
    group by community_area_id, date_year
),

yoy_comparison as (
    select
        curr.community_area_id,
        curr.date_year as current_year,
        round(curr.median_resolution_days, 2) as current_year_median_days,
        round(prev.median_resolution_days, 2) as previous_year_median_days,
        round(curr.median_resolution_days - prev.median_resolution_days, 2) as yoy_change_days,
        case 
            when curr.median_resolution_days < prev.median_resolution_days then 'Improved'
            when curr.median_resolution_days > prev.median_resolution_days then 'Worsened'
            else 'Unchanged'
        end as trend
    from yearly_medians curr
    left join yearly_medians prev
        on curr.community_area_id = prev.community_area_id
       and curr.date_year = prev.date_year + 1
)

select *
from yoy_comparison
order by current_year_median_days desc
