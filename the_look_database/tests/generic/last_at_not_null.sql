{% test last_acton_at_not_null(model, column_name, count_column) %}

    with dates as (
        select 
            {{column_name}} as dt 
        from {{model}}
        where {{count_column}} > 0
    ),
    
    error_rows as (
        select 
            *
        from dates 
        where dt is null
    )

    select * from error_rows

{% endtest %}