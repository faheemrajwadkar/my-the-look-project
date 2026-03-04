{% macro drop_ci_tables() %}
    
    {# Define the commands #}
    {% set drop_queries = [
        "DROP SCHEMA IF EXISTS CI_FRAJWADKAR_STAGING",
        "DROP SCHEMA IF EXISTS CI_FRAJWADKAR_SNAPSHOTS",
        "DROP SCHEMA IF EXISTS CI_FRAJWADKAR_MARTS"
    ] %}

    {% for query in drop_queries %}
        {{ log("Executing: " ~ query, info=True) }}
        {% do run_query(query) %}
    {% endfor %}

{% endmacro %}