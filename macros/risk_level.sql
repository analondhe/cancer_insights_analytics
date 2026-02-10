{% macro risk_level(score_column) %}
    case
        when {{ score_column }} >= {{ var('medium_risk_threshold') }} then 'High'
        when {{ score_column }} >= {{ var('low_risk_threshold') }} then 'Medium'
        else 'Low'
    end
{% endmacro %}