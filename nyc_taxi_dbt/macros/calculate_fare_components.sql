{% macro calculate_fare_components(airport_fee='0') %}
    COALESCE(fare_amount, 0)
    + COALESCE(extra, 0)
    + COALESCE(mta_tax, 0)
    + COALESCE(tip_amount, 0)
    + COALESCE(tolls_amount, 0)
    + COALESCE(improvement_surcharge, 0)
    + COALESCE(congestion_surcharge, 0)
    + COALESCE({{ airport_fee }}, 0)
    + COALESCE(cbd_congestion_fee, 0)
{% endmacro %}