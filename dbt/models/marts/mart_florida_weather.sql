{{ config(materialized='table') }}

-- Florida station-days with station attributes, for the Power BI report.
select
    f.station_id,
    d.station_name,
    d.latitude,
    d.longitude,
    d.elevation_m,
    d.network_code,
    f.obs_date,
    f.tmax_c,
    f.tmin_c,
    f.tavg_c,
    f.prcp_mm,
    f.diurnal_range_c,
    f.tmax_c * 9.0 / 5.0 + 32   as tmax_f,
    f.tmin_c * 9.0 / 5.0 + 32   as tmin_f,
    f.prcp_mm / 25.4            as prcp_in
from {{ ref('fct_station_day') }} f
join {{ ref('dim_station') }} d
  on d.station_id = f.station_id
 and d.is_current
where d.state = 'FL'
