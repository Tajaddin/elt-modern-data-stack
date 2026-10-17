{% test current_version_is_unique(model) %}

-- An SCD2 dimension must have at most one current row per natural key.
select station_id, count(*) as current_rows
from {{ model }}
where is_current
group by station_id
having count(*) > 1

{% endtest %}
