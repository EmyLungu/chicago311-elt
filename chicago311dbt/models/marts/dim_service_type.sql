with unique_types as (
    select distinct
        sr_type,
        sr_short_code,
        created_department,
        owner_department
    from {{ ref('stg_chicago311') }}
    where sr_type is not null
)

select
    row_number() over (order by sr_type, sr_short_code) as service_type_key,
    sr_type,
    sr_short_code,
    created_department,
    owner_department
from unique_types
