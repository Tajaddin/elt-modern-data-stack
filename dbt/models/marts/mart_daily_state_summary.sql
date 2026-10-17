{{ config(materialized='table') }}

-- Daily weather summary per US state: what a dashboard reads first.
select
    d.state,
    f.obs_date,
    count(*)                                   as reporting_stations,
    avg(f.tmax_c)                              as avg_tmax_c,
    avg(f.tmin_c)                              as avg_tmin_c,
    max(f.tmax_c)                              as max_tmax_c,
    min(f.tmin_c)                              as min_tmin_c,
    avg(f.prcp_mm)                             as avg_prcp_mm,
    sum(case when f.prcp_mm > 0 then 1 else 0 end) as stations_with_rain,
    max(f.prcp_mm)                             as max_prcp_mm
from {{ ref('fct_station_day') }} f
join {{ ref('dim_station') }} d
  on d.station_id = f.station_id
 and d.is_current
where d.state is not null
group by d.state, f.obs_date
