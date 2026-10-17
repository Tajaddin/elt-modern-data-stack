{% snapshot snap_stations %}

{{
    config(
        unique_key='station_id',
        strategy='check',
        check_cols=['station_name', 'state', 'latitude', 'longitude', 'elevation_m', 'gsn_flag', 'hcn_crn_flag', 'wmo_id'],
        invalidate_hard_deletes=true
    )
}}

-- Snapshot of the latest station metadata. dbt opens a new version when any checked column changes.
select
    station_id,
    station_name,
    state,
    country_code,
    network_code,
    latitude,
    longitude,
    elevation_m,
    gsn_flag,
    hcn_crn_flag,
    wmo_id
from {{ ref('stg_ghcn__stations') }}

{% endsnapshot %}
