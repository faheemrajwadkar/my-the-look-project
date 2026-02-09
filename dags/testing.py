# from airflow.sdk import dag, chain
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
# from cosmos.constants import InvocationMode


# from cosmos.profiles.snowflake import SnowflakeUserPasswordProfileMapping
import os 
from os import walk

# DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/the_look_database"

DBT_PROJECT_PATH = f"/workspaces/my-the-look-project/the_look_database"

files_in_staging = os.listdir(DBT_PROJECT_PATH + f"/models/staging/the_look_ecommerce")

staging_models = [e.replace('.sql', '') for e in files_in_staging if e.endswith('.sql')]

files_in_marts = os.listdir(DBT_PROJECT_PATH + f"/models/marts/")

marts_models = [e.replace('.sql', '') for e in files_in_marts if e.endswith('.sql')]

print(staging_models + marts_models)
