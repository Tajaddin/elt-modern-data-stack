{{ config(materialized='view') }}

-- The most recent station snapshot per station. The SCD2 history lives in the snapshot table.
with ranked as (
    select
        *,
        row_number() over (partition by station_id order by loaded_at desc, run_id desc) as version_rank
    from {{ source('raw', 'ghcn_stations') }}
)

select
    station_id,
    latitude,
    longitude,
    elevation_m,
    state,
    name                              as station_name,
    gsn_flag,
    hcn_crn_flag,
    wmo_id,
    substr(station_id, 1, 2)          as country_code,
    substr(station_id, 3, 1)          as network_code,
    run_id,
    loaded_at
from ranked
where version_rank = 1
