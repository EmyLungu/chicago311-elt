{% snapshot snp_dim_ward %}

{{
    config(
        target_schema='snapshots',
        unique_key='ward_map_key',
        strategy='timestamp',
        updated_at='effective_from'
    )
}}

with wards as (
    select distinct ward_id from {{ ref('stg_chicago311') }} where ward_id is not null
),

map_versions as (
    select * from {{ ref('ward_map_versions') }}
)

select
    w.ward_id || '_' || m.map_version as ward_map_key,
    w.ward_id,
    m.map_version,
    m.effective_from::timestamp as effective_from,
    m.effective_to::timestamp as effective_to
from wards w
cross join map_versions m

{% endsnapshot %}
