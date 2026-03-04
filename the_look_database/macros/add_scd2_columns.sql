{% macro add_scd2_columns(created_at_present) %}

{{ dbt_utils.generate_surrogate_key(["id", "dbt_valid_from"]) }} as version_sk,
case 
    when dbt_valid_from = (select min(dbt_valid_from) from source where id = s.id) 
    then 
        {% if created_at_present == 1 %}
            {{ cast_as_timestamp("created_at") }} 
        {% elif created_at_present == 0 %}
            to_timestamp('2018-01-01')
        {% endif %}
    else dbt_valid_from 
end as valid_from,
dbt_valid_to as valid_to

{% endmacro %}