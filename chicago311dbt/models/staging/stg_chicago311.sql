select
    -- Identifiers
    nullif(trim(sr_number), '') as sr_number,
    nullif(trim(parent_sr_number), '') as parent_sr_number,
    nullif(trim(legacy_sr_number), '') as legacy_sr_number,

    -- Categorical
    upper(trim(sr_type)) as sr_type,
    upper(trim(sr_short_code)) as sr_short_code,
    upper(trim(status)) as status,
    upper(trim(origin)) as origin,
    upper(trim(created_department)) as created_department,
    upper(trim(owner_department)) as owner_department,
    
    -- Timestamps
    created_date::timestamp as created_date,
    last_modified_date::timestamp as last_modified_date,
    closed_date::timestamp as closed_date,
    -- Time parts
    created_hour::int as created_hour,
    created_day_of_week::int as created_day_of_week,
    created_month::int as created_month,

    -- Booleans
    coalesce(duplicate::boolean, false) as is_duplicate,
    coalesce(legacy_record::boolean, false) as is_legacy_record,

    -- Address
    trim(street_address) as street_address,
    upper(trim(street_number)) as street_number,
    upper(trim(street_direction)) as street_direction,
    upper(trim(street_name)) as street_name,
    upper(trim(street_type)) as street_type,
    upper(trim(city)) as city,
    upper(trim(state)) as state,
    nullif(trim(zip_code), '') as zip_code,
    -- Administrative
    ward::int as ward_id,
    community_area::int as community_area_id,
    nullif(trim(electrical_district), '') as electrical_district,
    nullif(trim(electricity_grid), '') as electricity_grid,
    nullif(trim(police_sector), '') as police_sector,
    nullif(trim(police_district), '') as police_district,
    nullif(trim(police_beat), '') as police_beat,
    nullif(trim(precinct), '') as precinct,
    nullif(trim(sanitation_division_days), '') as sanitation_division_days,
    -- Coordinates
    x_coordinate::double as x_coordinate,
    y_coordinate::double as y_coordinate,
    latitude::double as latitude,
    longitude::double as longitude,
    location as location_raw
from {{ source ('chicago311_raw', 'raw_chicago311')}}
