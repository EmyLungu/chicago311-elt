select
    ward_map_key,
    ward_id,
    map_version,
    effective_from,
    effective_to,
    dbt_valid_from,
    coalesce(dbt_valid_to, '9999-12-31 23:59:59'::timestamp) as dbt_valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snp_dim_ward') }}
