{{ config(materialized='view') }}

-- Latest loaded copy of every (station, date, element); NOAA revises recent days and overlapping
-- runs re-load them, so raw holds several versions and the newest wins here.
with ranked as (
    select
        station_id,
        obs_date,
        element,
        data_value,
        m_flag,
        q_flag,
        s_flag,
        obs_time,
        run_id,
        loaded_at,
        row_number() over (
            partition by station_id, obs_date, element
            order by loaded_at desc, run_id desc
        ) as version_rank
    from {{ source('raw', 'ghcn_observations') }}
)

select
    station_id,
    obs_date,
    element,
    data_value                                   as raw_value,
    case
        when element in ('TMAX', 'TMIN', 'TAVG', 'TOBS') then data_value / 10.0   -- tenths of degrees C
        when element = 'PRCP' then data_value / 10.0                               -- tenths of mm
        when element in ('SNOW', 'SNWD') then data_value * 1.0                     -- mm
        when element = 'AWND' then data_value / 10.0                               -- tenths of m/s
        else data_value * 1.0
    end                                          as value,
    case
        when element in ('TMAX', 'TMIN', 'TAVG', 'TOBS') then 'degC'
        when element in ('PRCP', 'SNOW', 'SNWD') then 'mm'
        when element = 'AWND' then 'm/s'
        else 'raw'
    end                                          as unit,
    m_flag,
    q_flag,
    s_flag,
    obs_time,
    q_flag is not null                           as failed_quality_check,
    run_id,
    loaded_at
from ranked
where version_rank = 1
