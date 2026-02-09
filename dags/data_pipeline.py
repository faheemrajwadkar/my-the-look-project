from airflow.sdk import dag, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import InvocationMode


from cosmos.profiles.snowflake import SnowflakeUserPasswordProfileMapping
import os 
import pendulum
import datetime

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/the_look_database"

profile_config = ProfileConfig(
    profile_name = "default",
    target_name = "dev",
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
        dag_id = "__data_pipeline",
        schedule = None,
        start_date = pendulum.datetime(2024, 1, 1, tz = "UTC"),
        catchup = False,
        dagrun_timeout=datetime.timedelta(minutes=60),
)
def run_data_pipeline():
    project_dag = DbtTaskGroup(
        group_id = "__data_pipeline",
        profile_config = profile_config,
        project_config = project_config,
        execution_config = execution_config,
    )

dag = run_data_pipeline()