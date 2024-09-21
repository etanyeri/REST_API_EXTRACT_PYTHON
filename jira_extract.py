import sys
import os
import json
import logging

sys.path.append("/user/local/airflow/dags/")
sys.path.append(os.path.dirname(__file__))

from jira.get_jira_data import pull_jira_data
from airflow.models import variable
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import utils
import dag_utils


if os.environ.get("CI"):
  S3_BUCKET_ROOT = "TEST_S3_BUCKET"
  S3_BRANCH_ALIAS = "MR"
  AIRFLOW__WEBSERVER__BASE_URL = "URL"
else:  
  S3_BUCKET_ROOT = Variable.get("S3_INGEST_BUCKET", default_var=None)
  S3_BRANCH_ALIAS = Variable.get("S3_BRANCH_ALIAS", default_var=None)
  AIRFLOW__WEBSERVER__BASE_URL = Variable.get("AIRFLOW_WEBSERVER_BASE_URL", default_var=None)

# Fetching Airflow Variables and setting environment variables
os.environ["ENV"] = Variable.get("ENV", default_var=None)
os.environ["PROFILES_PATH"] = Variable.get("PROFILES_PATH", default_var=None)
os.environ["DP_SLACK_NOTICE_URLS"] = Variable.get("DP_SLACK_NOTICE_URLS", default_var=None)
os.environ["DBT_ENV_USESSM"] = Variable.get("DBT_ENV_USESSM", default_var=None)
os.environ["DBT_ENV_NAME"] = Variable.get("DBT_ENV_NAME", default_var=None)
os.environ["JIRA_INGEST_TARGET_DATABASE"] = Variable.get("JIRA_INGEST_TARGET_DATABASE", default_var=None)
os.environ["JIRA_API_ELEMENT_LIST"] = Variable.get("JIRA_API_ELEMENT_LIST", default_var=None)
os.environ["JIRA_INGEST_SNOWFLAKE_USESSM"] = Variable.get("JIRA_INGEST_SNOWFLAKE_USESSM", default_var=None)
os.environ["JIRA_API_ENV"] = Variable.get("JIRA_API_ENV", default_var=None)
os.environ["JIRA_SNOWFLAKE_USER"] = Variable.get("JIRA_SNOWFLAKE_USER", default_var=None)
os.environ["JIRA_SNOWFLAKE_PASSWORD"] = Variable.get("JIRA_SNOWFLAKE_PASSWORD", default_var=None)
os.environ["JIRA_SNOWFLAKE_ROLE"] = Variable.get("JIRA_SNOWFLAKE_ROLE", default_var=None)
os.environ["JIRA_SNOWFLAKE_WAREHOUSE"] = Variable.get("JIRA_SNOWFLAKE_WAREHOUSE", default_var=None)
os.environ["JIRA_SNOWFLAKE_DATABASE"] = Variable.get("JIRA_SNOWFLAKE_DATABASE", default_var=None)

# Function to pull data from Jira
def pull_jira_data(**kwargs):
    # Function logic here
    pass

# Function to ingest data into S3
def ingest_data_to_s3(**kwargs):
    # Function logic to ingest data into S3
    pass

# Define the DAG
dag = DAG(
    'jira_ingestion_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(2),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Ingest data from Jira to S3',
    schedule_interval='@daily',
)

# Task for pulling Jira data
task_pull = PythonOperator(
    task_id='pull_jira_data',
    python_callable=pull_jira_data,
    dag=dag,
)

# Task for ingesting data into S3
task_ingest = PythonOperator(
    task_id='ingest_jira_data',
    python_callable=ingest_data_to_s3,
    dag=dag,
)

# Load config and process tables
with open('/configs/jira_ingest_config.json', 'r') as f:
    config = json.load(f)

# Reading the source tables and configuring them
source_tables = config['source_tables']

for table in source_tables:
    # Logic to process each table
    pass

# Set logging level
logging.getLogger().setLevel(logging.INFO)


  
  
