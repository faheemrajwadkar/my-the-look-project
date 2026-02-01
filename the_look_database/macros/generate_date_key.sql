{% macro generate_date_key(date_column) %}

    CAST(REPLACE(CAST(DATE({{ date_column }}) as VARCHAR(10)), '-', '') as INT)

{% endmacro %}