{{ config(materialized='view') }}

-- Pivot the long element table into one row per station-day. Rows with a NOAA quality flag are dropped.
select
    station_id,
    obs_date,
    max(case when element = 'TMAX' then value end)  as tmax_c,
    max(case when element = 'TMIN' then value end)  as tmin_c,
    max(case when element = 'TAVG' then value end)  as tavg_c,
    max(case when element = 'PRCP' then value end)  as prcp_mm,
    max(case when element = 'SNOW' then value end)  as snow_mm,
    max(case when element = 'SNWD' then value end)  as snwd_mm,
    max(case when element = 'AWND' then value end)  as awnd_ms,
    count(*)                                        as n_elements,
    max(loaded_at)                                  as last_loaded_at
from {{ ref('stg_ghcn__observations') }}
where not failed_quality_check
group by station_id, obs_date
