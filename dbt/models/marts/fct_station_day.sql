{{
    config(
        materialized='incremental',
        unique_key=['station_id', 'obs_date'],
        incremental_strategy=('merge' if target.type in ('databricks', 'snowflake') else 'delete+insert'),
        on_schema_change='append_new_columns'
    )
}}

-- Station-day fact. Incremental: each run re-merges a window of days so late NOAA revisions land; the
-- window comes from the run (vars window_start, window_end) or is the last lookback_days days held.
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
where
    {% if var('window_start') is none -%}
    obs_date >= (
        select {{ dbt.dateadd('day', -var('lookback_days'), "coalesce(max(obs_date), cast('1900-01-01' as date))") }}
        from {{ this }}
    )
    {%- else -%}
    obs_date >= cast('{{ var("window_start") }}' as date)
    {%- endif %}
    {%- if var('window_end') is not none %}
    and obs_date <= cast('{{ var("window_end") }}' as date)
    {%- endif %}
{% endif %}
