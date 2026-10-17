-- Volume drift: the newest load must be within volume_band_factor of the median of the previous runs.
-- Catches a silently truncated extract (too few rows) and a runaway one (too many). Needs 3+ earlier runs.
with runs as (
    select run_id, run_date, rows_loaded, loaded_at,
           row_number() over (order by loaded_at desc) as recency
    from {{ source('raw', 'load_runs') }}
),
latest as (
    select * from runs where recency = 1
),
baseline as (
    select median(rows_loaded) as median_rows, count(*) as n_runs
    from runs
    where recency between 2 and 31
)
select latest.run_id, latest.rows_loaded, baseline.median_rows, baseline.n_runs
from latest, baseline
where baseline.n_runs >= 3
  and (
        latest.rows_loaded > baseline.median_rows * {{ var('volume_band_factor') }}
     or latest.rows_loaded < baseline.median_rows / {{ var('volume_band_factor') }}
  )
