{% test unique_combination(model, combination) %}

-- Combinations of columns that appear more than once.
select {{ combination | join(', ') }}, count(*) as n
from {{ model }}
group by {{ combination | join(', ') }}
having count(*) > 1

{% endtest %}
