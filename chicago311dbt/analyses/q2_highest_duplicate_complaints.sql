with ranked_complaints as (
    select
        f.sr_number,
        s.sr_type,
        f.street_address,
        f.created_date,
        lag(f.created_date) over (
            partition by s.sr_type, f.street_address
            order by f.created_date
        ) as prev_created_date
    from {{ ref('fct_service_requests') }} f
    join {{ ref('dim_service_type') }} s 
        on f.service_type_key = s.service_type_key
    where f.street_address is not null
),

flagged_duplicates as (
    select
        sr_number,
        sr_type,
        case 
            when prev_created_date is not null 
             and date_diff('day', prev_created_date, created_date) <= 3 
            then 1 
            else 0 
        end as is_short_window_duplicate
    from ranked_complaints
)

select
    sr_type,
    count(*) as total_requests,
    sum(is_short_window_duplicate) as duplicate_requests,
    round(
        cast(sum(is_short_window_duplicate) as double) / count(*), 4
    ) as duplicate_ratio
from flagged_duplicates
group by sr_type
having count(*) >= 100
order by duplicate_ratio desc
