{% macro age_bucket(age_column) %}
    case
        when {{ age_column }} < {{ var('age_groups')['young'] }} then 'Young'
        when {{ age_column }} between {{ var('age_groups')['young'] }} and {{ var('age_groups')['middle'] }} then 'Middle-aged'
        when {{ age_column }} between {{ var('age_groups')['middle'] + 1 }} and {{ var('age_groups')['senior'] }} then 'Senior'
        when {{ age_column }} > {{ var('age_groups')['senior'] }} then 'Elderly'
        else 'Unknown'
    end
{% endmacro %}