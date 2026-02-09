from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import dag, task
from datetime import datetime


def get_table_metadata(database_name, schema_name):
    hook = SnowflakeHook(
        snowflake_conn_id = "snowflake_conn",
    )

    db = database_name.upper()
    sn = schema_name.upper()

    query = f"""
        select table_name, max(ordinal_position) - 2 as column_count
        from {db}.information_schema.columns
        where table_schema = '{sn}'
        group by 1;
    """
    records = hook.get_records(query)

    return records