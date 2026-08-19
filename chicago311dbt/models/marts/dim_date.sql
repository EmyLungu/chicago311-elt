with date_spine as (
    select cast('2018-01-01' as date) + interval (i) day as date_day
    from range(0, 365 * 15) as t(i)
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    extract(year from date_day) as date_year,
    extract(quarter from date_day) as date_quarter,
    extract(month from date_day) as date_month,
    strftime(date_day, '%B') as month_name,
    strftime(date_day, '%b') as month_name_short,
    extract(day from date_day) as day_of_month,
    extract(dayofweek from date_day) as day_of_week,
    strftime(date_day, '%A') as day_name,
    case when extract(dayofweek from date_day) in (0, 6) then true else false end as is_weekend
from date_spine
