{{ config(materialized='table') }}

-- Per station: how much of the loaded period it reported, and what it reports. Feeds the coverage page.
select
    f.station_id,
    d.station_name,
    d.state,
    d.network_code,
    min(f.obs_date)                                   as first_obs_date,
    max(f.obs_date)                                   as last_obs_date,
    count(*)                                          as days_reported,
    sum(case when f.tmax_c is not null then 1 else 0 end)  as days_with_tmax,
    sum(case when f.prcp_mm is not null then 1 else 0 end) as days_with_prcp
from {{ ref('fct_station_day') }} f
left join {{ ref('dim_station') }} d
  on d.station_id = f.station_id
 and d.is_current
group by f.station_id, d.station_name, d.state, d.network_code
