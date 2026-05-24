{% macro revenue_per(numerator, denominator) %}
    SAFE_DIVIDE({{ numerator }}, NULLIF({{ denominator }}, 0))
{% endmacro %}