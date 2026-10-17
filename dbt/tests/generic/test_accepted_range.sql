{% test accepted_range(model, column_name, min_value=none, max_value=none) %}

-- Rows whose value falls outside [min_value, max_value]. Nulls pass (use not_null for that).
select {{ column_name }} as offending_value
from {{ model }}
where {{ column_name }} is not null
{% if min_value is not none %}
  and {{ column_name }} < {{ min_value }}
{% endif %}
{% if max_value is not none %}
  and {{ column_name }} > {{ max_value }}
{% endif %}

{% endtest %}
