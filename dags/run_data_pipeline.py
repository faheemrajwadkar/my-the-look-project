# from airflow.decorators import dag, task
from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import InvocationMode
from cosmos.profiles.snowflake import SnowflakeUserPasswordProfileMapping

from include.utils.ingestion_helpers import get_table_metadata
from include.utils.alerts import slack_failure_callback

import os 
import pendulum
import datetime

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/the_look_database"

profile_config = ProfileConfig(
    profile_name = "default",
    target_name = "prod",
    profile_mapping = SnowflakeUserPasswordProfileMapping(
        conn_id = "snowflake_conn",
        profile_args={
            "schema": "DBT_FRAJWADKAR",
        }
    )
)

project_config = ProjectConfig(
    dbt_project_path = DBT_PROJECT_PATH,
)

execution_config = ExecutionConfig(
    invocation_mode=InvocationMode.DBT_RUNNER,
)

@dag(
        dag_id = "00__run_data_pipeline",
        schedule = None,
        start_date = pendulum.datetime(2024, 1, 1, tz = "UTC"),
        catchup = False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        on_failure_callback=slack_failure_callback,
        tags=["core", "raw+models"],
)
def run_data_pipeline():

    @task()
    def data_ingestion():
        # 1. Fetch metadata (returns list of tuples)
        raw_metadata = get_table_metadata('RAW', 'THE_LOOK_ECOMMERCE')
        
        # 2. Convert list of tuples to dict: {'table_name': col_count}
        tables_dict = {row[0]: row[1] for row in raw_metadata}

        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn"
        )
        
        for table_name, col_count in tables_dict.items():
            # Generate the t.$1, t.$2... string
            columns_sql = ", ".join([f"t.${i+1}" for i in range(int(col_count))])
            
            # Use fully qualified names to be safe
            copy_query = f"""
                COPY INTO RAW.THE_LOOK_ECOMMERCE.{table_name} FROM (
                    SELECT 
                        {columns_sql}, 
                        SYSDATE(), 
                        METADATA$FILENAME
                    FROM @RAW.THE_LOOK_ECOMMERCE.GCS_THE_LOOK_STAGE/{table_name.lower()}/ (file_format => 'RAW.THE_LOOK_ECOMMERCE.THE_LOOK_CSV_FORMAT') t
                )
                ON_ERROR = 'ABORT_STATEMENT';
            """
            print(f"Executing copy for {table_name}...")
            hook.run(copy_query)

    def data_transformation():
        project_dag = DbtTaskGroup(
            group_id = "__data_transformations",
            profile_config = profile_config,
            project_config = project_config,
            execution_config = execution_config,
        )

        return project_dag
    
    data_ingestion() >> data_transformation()

run_data_pipeline()