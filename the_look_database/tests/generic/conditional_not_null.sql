{% test conditional_not_null(model, column_name, condition) %}

    with filtered_rows as (
        select 
            {{column_name}} as col 
        from {{model}}
        where ({{condition}})
    ),
    
    error_rows as (
        select 
            *
        from filtered_rows 
        where col is null
    )

    select * from error_rows

{% endtest %}