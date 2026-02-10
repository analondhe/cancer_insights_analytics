{% macro age_bucket(column_name) %}
    case
        when {{ column_name }} < 20 then 'Under 20'
        when {{ column_name }} between 20 and 29 then '20-29'
        when {{ column_name }} between 30 and 39 then '30-39'
        when {{ column_name }} between 40 and 49 then '40-49'
        when {{ column_name }} between 50 and 59 then '50-59'
        when {{ column_name }} >= 60 then '60+'
        else 'Unknown'
    end
{% endmacro %}
