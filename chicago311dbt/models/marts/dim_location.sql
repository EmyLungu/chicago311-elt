with unique_locations as (
    select distinct
        community_area_id,
        zip_code,
        police_district,
        police_sector,
        police_beat,
        precinct,
        sanitation_division_days,
        electrical_district,
        electricity_grid
    from {{ ref('stg_chicago311') }}
    where community_area_id is not null or zip_code is not null
)

select
    row_number() over (
        order by community_area_id, zip_code, police_beat
    ) as location_key,
    community_area_id,
    zip_code,
    police_district,
    police_sector,
    police_beat,
    precinct,
    sanitation_division_days,
    electrical_district,
    electricity_grid
from unique_locations
