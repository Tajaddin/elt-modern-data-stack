{{ config(materialized='table') }}

-- SCD Type 2 station dimension built from the dbt snapshot: one row per station per version.
select
    station_id,
    dbt_scd_id                       as station_version_key,
    station_name,
    state,
    country_code,
    network_code,
    latitude,
    longitude,
    elevation_m,
    gsn_flag,
    hcn_crn_flag,
    wmo_id,
    dbt_valid_from                   as valid_from,
    dbt_valid_to                     as valid_to,
    dbt_valid_to is null             as is_current
from {{ ref('snap_stations') }}
