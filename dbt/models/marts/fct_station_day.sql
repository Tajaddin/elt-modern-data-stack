{{
    config(
        materialized='incremental',
        unique_key=['station_id', 'obs_date'],
        incremental_strategy=('merge' if target.type in ('databricks', 'snowflake') else 'delete+insert'),
        on_schema_change='append_new_columns',
        pre_hook="{% if is_incremental() %}delete from {{ this }} where {{ station_day_window() }}{% endif %}"
    )
}}

-- Station-day fact. Incremental: each run rebuilds a window of days (macros/station_day_window.sql). The
-- pre-hook deletes the window and the select fills it again, so late NOAA revisions land and a station-day
-- whose every element NOAA has since flagged drops out instead of keeping its old values.
select
    station_id,
    obs_date,
    tmax_c,
    tmin_c,
    tavg_c,
    prcp_mm,
    snow_mm,
    snwd_mm,
    awnd_ms,
    case when tmax_c is not null and tmin_c is not null then tmax_c - tmin_c end as diurnal_range_c,
    n_elements,
    last_loaded_at
from {{ ref('int_station_day') }}

{% if is_incremental() %}
where {{ station_day_window() }}
{% endif %}
