from airflow.sdk import dag, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import InvocationMode


from cosmos.profiles.snowflake import SnowflakeUserPasswordProfileMapping
import os 
import pendulum
import datetime

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/the_look_database"
manifest_path=f"{DBT_PROJECT_PATH}/target/manifest.json"

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
    manifest_path = manifest_path,
)

execution_config = ExecutionConfig(
    invocation_mode=InvocationMode.DBT_RUNNER,
)

# getting list of models
files_in_staging = os.listdir(DBT_PROJECT_PATH + f"/models/staging/the_look_ecommerce")
staging_models = [e.replace('.sql', '') for e in files_in_staging if e.endswith('.sql')]

files_in_marts = os.listdir(DBT_PROJECT_PATH + f"/models/marts/")
marts_models = [e.replace('.sql', '') for e in files_in_marts if e.endswith('.sql')]

models = staging_models + marts_models

# --- DAG Generation Loop ---
for model in models:
    dag_id = f"dbt_{model}"  # Unique ID for each DAG
    
    globals()[dag_id] = DbtDag(
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            select=[f"+{model}"]  # This isolates the DAG to just this model
        ),
        dag_id=dag_id,
        schedule=None,
        start_date=pendulum.datetime(2024, 1, 1),
        catchup=False,
        tags=["individual"],
    )
