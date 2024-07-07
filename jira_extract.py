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
  
