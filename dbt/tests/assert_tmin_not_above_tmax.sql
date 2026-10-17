-- A station-day where the minimum exceeds the maximum is a data error, not weather.
select station_id, obs_date, tmax_c, tmin_c
from {{ ref('fct_station_day') }}
where tmax_c is not null
  and tmin_c is not null
  and tmin_c > tmax_c + 0.5
