{% test aggregates_match(model, column_name, main_function, source_model, source_column_name, source_function) %}

    with source as (
        select 
            {{source_function}}({{source_column_name}}) as source_agg
        from {{source_model}}
    ),

    main as (
        select 
            {{main_function}}({{column_name}}) as main_agg
        from {{model}}
    )

    select 
        s.source_agg,
        m.main_agg
    from source s
    cross join main m
    where round(source_agg, 3) != round(main_agg, 3)

{% endtest %}