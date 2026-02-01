{% macro cast_as_timestamp(string_timestamp_col) %}

    COALESCE(
        TRY_TO_TIMESTAMP_NTZ({{ string_timestamp_col }}, 'YYYY-MM-DD HH:MI:SS UTC'),
        TRY_TO_TIMESTAMP_NTZ({{ string_timestamp_col }}, 'YYYY-MM-DD HH:MI:SS.FF UTC')
    )

{% endmacro %}