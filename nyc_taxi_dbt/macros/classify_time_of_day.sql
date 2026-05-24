{% macro classify_time_of_day(column_name) %}
    CASE
        WHEN EXTRACT(HOUR FROM {{ column_name }}) BETWEEN 7 AND 9   THEN 'morning_rush'
        WHEN EXTRACT(HOUR FROM {{ column_name }}) BETWEEN 17 AND 19 THEN 'evening_rush'
        WHEN EXTRACT(HOUR FROM {{ column_name }}) BETWEEN 0 AND 5   THEN 'overnight'
        ELSE 'off_peak'
    END
{% endmacro %}