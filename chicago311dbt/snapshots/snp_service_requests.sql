{% snapshot snp_service_requests %}

{{
    config(
        target_schema='snapshots',
        unique_key='sr_number',
        strategy='check',
        check_cols=['status', 'closed_date', 'last_modified_date']
    )
}}

select
    sr_number,
    status,
    created_date,
    closed_date,
    last_modified_date,
    sr_type,
    sr_short_code,
    owner_department,
    ward_id,
    community_area_id
from {{ ref('stg_chicago311')}}

{% endsnapshot %}
