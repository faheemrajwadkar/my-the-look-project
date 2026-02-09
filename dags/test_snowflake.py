from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id='test_snowflake_connection',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    test_query = SQLExecuteQueryOperator(
        task_id='run_test_query',
        conn_id='snowflake_conn', 
        sql='SELECT CURRENT_VERSION();'
    )