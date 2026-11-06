{#-
    The days an incremental build of fct_station_day rebuilds, as a predicate on obs_date.

    window_start and window_end come from the run (see dbt_project.yml). Without window_start the lower
    bound is lookback_days before the latest day the fact holds, read once into a literal: the model's
    pre-hook deletes the range before its select runs, and both have to see the same range.
-#}
{% macro station_day_window() -%}
    {%- set start = var('window_start') -%}
    {%- if start is none and execute -%}
        {%- set latest = run_query('select max(obs_date) from ' ~ this).columns[0].values()[0] -%}
        {%- if latest is not none -%}
            {%- set start = (latest - modules.datetime.timedelta(days=var('lookback_days'))).strftime('%Y-%m-%d') -%}
        {%- endif -%}
    {%- endif -%}
    obs_date >= cast('{{ start or "1900-01-01" }}' as date)
    {%- if var('window_end') is not none %} and obs_date <= cast('{{ var("window_end") }}' as date){% endif %}
{%- endmacro %}
