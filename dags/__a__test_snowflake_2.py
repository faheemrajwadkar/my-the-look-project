from airflow import DAG
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id='__a__test_snowflake_2',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    test_query = SQLExecuteQueryOperator(
        task_id='__a__test_snowflake_2',
        conn_id='snowflake_conn', 
        sql='SELECT CURRENT_VERSION();',
        params={
            "database": "ANALYTICS",
            "schema": "DBT_FRAJWADKAR_STAGING"
        }
    )