import sys
import os
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from FlatFileManagement.google_sheets_api import pull_spreadsheets_from_drive
import utils, logging, dag_utils
from airflow.models import Variable

# Add paths to the system path
sys.path.append("/usr/local/airflow/dags/")
sys.path.append(os.path.dirname(__file__))

# Environment check: If running in Continuous Integration (CI) environment
if os.environ.get("CI"):
    # Set default values for test environment
    S3_BUCKET_ROOT = "TEST_BUCKET"
    S3_BRANCH_ALIAS = "MR"
    AIRFLOW__WEBSERVER__BASE_URL = "URL"
else:
    # Fetch values from Airflow Variables for production environment
    S3_BUCKET_ROOT = Variable.get("S3_INGEST_BUCKET_ROOT")
    S3_BRANCH_ALIAS = Variable.get("S3_BRANCH_ALIAS")
    AIRFLOW__WEBSERVER__BASE_URL = Variable.get("AIRFLOW__WEBSERVER__BASE_URL")

    # Set Snowflake credentials
    os.environ["DBT_ENV_CUSTOM_ENV_SNOWFLAKE_USER"] = Variable.get("DI_SNOWFLAKE_USERNAME")
    os.environ["DBT_ENV_SECRET_SNOWFLAKE_PASSWORD"] = Variable.get("DI_SNOWFLAKE_PASSWORD")
    os.environ["DBT_ENV_CUSTOM_ENV_SNOWFLAKE_ROLE"] = Variable.get("DI_SNOWFLAKE_ROLE")
    os.environ["DBT_ENV_CUSTOM_ENV_SNOWFLAKE_WAREHOUSE"] = Variable.get("DBT_SNOWFLAKE_WAREHOUSE_DI")
    os.environ["DBT_ENV_CUSTOM_ENV_INGEST_DATABASE"] = Variable.get("DBT_ENV_CUSTOM_ENV_INGEST_DATABASE")
    os.environ["DBT_ENV_CUSTOM_ENV_INGEST_TARGET_DATABASE"] = Variable.get("DBT_ENV_CUSTOM_ENV_INGEST_TARGET_DATABASE")

    # Set more Snowflake credentials for Google Drive ingestion
    os.environ["SNOWFLAKE_USERNAME"] = Variable.get("MWAA_GDRIVE_SNOWFLAKE_USERNAME")
    os.environ["SNOWFLAKE_PASSWORD"] = Variable.get("MWAA_GDRIVE_SNOWFLAKE_PASSWORD")
    os.environ["SNOWFLAKE_ROLE"] = Variable.get("GDRIVE_SNOWFLAKE_ROLE")
    os.environ["SNOWFLAKE_WAREHOUSE"] = Variable.get("GDRIVE_SNOWFLAKE_WAREHOUSE")
    os.environ["SNOWFLAKE_INGEST_GDRIVE_DATABASE"] = Variable.get("SNOWFLAKE_INGEST_DATABASE")

    # Set Google Drive credentials
    os.environ["GDRIVE_INGEST_PRIVATE_KEY_ID"] = Variable.get("MWAA_GDRIVE_INGEST_PRIVATE_KEY_ID")
    os.environ["GDRIVE_INGEST_PRIVATE_KEY"] = Variable.get("MWAA_GDRIVE_INGEST_PRIVATE_KEY")
    os.environ["GDRIVE_INGEST_CLIENT_ID"] = Variable.get("MWAA_GDRIVE_INGEST_CLIENT_ID")


# DAG definition and task scheduling

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    "gdrive_to_snowflake_pipeline",
    default_args=default_args,
    description="Pulls data from Google Drive and uploads it to Snowflake",
    schedule_interval=timedelta(days=1),  # Daily schedule
    start_date=days_ago(1),  # The DAG will start running from 1 day ago
    catchup=False,
)

# Python function to pull data from Google Sheets
def pull_data_from_gdrive():
    logging.info("Pulling data from Google Drive")
    sheets_data = pull_spreadsheets_from_drive()  # This would call your Google Sheets API function
    logging.info("Data pulled successfully")
    # Additional logic to process the data and upload it to Snowflake would go here
    return sheets_data

# Define tasks using PythonOperator

# Task to pull data from Google Drive
pull_gdrive_data_task = PythonOperator(
    task_id="pull_data_from_gdrive",
    python_callable=pull_data_from_gdrive,
    dag=dag,
)

# Example of a downstream task (optional)
def process_data_task():
    logging.info("Processing data pulled from Google Drive")
    # Processing logic goes here
    return True

# Task to process the data
process_data = PythonOperator(
    task_id="process_data",
    python_callable=process_data_task,
    dag=dag,
)

# Define the task order (dependencies)
pull_gdrive_data_task >> process_data
