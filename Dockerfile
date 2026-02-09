FROM astrocrpublic.azurecr.io/runtime:3.1-12

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && pip install --no-cache-dir dbt-snowflake && deactivate