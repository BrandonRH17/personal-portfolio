{% macro clean_email(column) %}
    case
        when {{ column }} like '%@%.%'
        then lower(trim({{column}}))
        else null
        end
{% endmacro %}