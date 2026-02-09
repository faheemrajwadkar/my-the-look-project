from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from include.utils.ingestion_helpers import get_table_metadata
from datetime import datetime

@dag(
    dag_id="01__refresh_raw_tables",
    schedule=None, 
    start_date=datetime(2024, 1, 1), 
    catchup=False,
    tags=["core", "raw"],
)
def daily_ingestion_and_dbt():

    @task
    def dynamic_ingest():
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

    dynamic_ingest()

# Just call the function
daily_ingestion_and_dbt()