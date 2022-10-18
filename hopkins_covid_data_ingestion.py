import os
import yaml

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta, datetime
from google.cloud import bigquery

from airflow.providers.github.operators.github import GithubOperator
import logging
from airflow.operators.python_operator import PythonOperator

# load configurations from yaml
config_file_path = os.path.join(os.path.dirname(__file__), 'configs/hopkins_covid_data_ingestion_configs.yaml')
with open(config_file_path) as config_yaml:
    CONFIGS = yaml.load(config_yaml, Loader=yaml.FullLoader)

airflow_configs = CONFIGS['airflow']
dataflow_configs = CONFIGS['dataflow']
job_name = airflow_configs['dag']['job_name']


def prepare_file_pattern(**context):
    # prune correction with one day, inorder to sync with the github update
    context_date = (datetime.strptime('2022-10-17', '%Y-%m-%d') - timedelta(days=1)).strftime('%m-%d-%Y')
    file_pattern = f"{dataflow_configs['data_folder']}{context_date}.csv"
    logging.info(f"file pattern saved: {file_pattern}")
    context['ti'].xcom_push(key='cvs_file_pattern', value=file_pattern)


default_args = {
    'owner': airflow_configs['default_args']['owner'],
    'depends_on_past': airflow_configs['default_args']['depends_on_past'],
    'start_date': airflow_configs['default_args']['start_date'],
    'email': ['airflow@example.com'],
    'email_on_failure': airflow_configs['default_args']['email_on_failure'],
    'email_on_retry': airflow_configs['default_args']['email_on_retry'],
    'retries': airflow_configs['default_args']['retries'],
    'retry_delay': airflow_configs['default_args']['retry_delay'],
    'start_date': eval(airflow_configs['default_args']['start_date']),
    'max_active_runs': 1
}

dag = DAG(
    airflow_configs['dag']['job_name'],
    default_args=default_args,
    description=airflow_configs['dag']['description'],
    schedule_interval=airflow_configs['dag']['schedule_interval'],
    tags=['HOPKINS', 'COVID_DATA', 'MEDICAL_DATA', 'COVID_WORLD']
)

get_context_date_for_csv = PythonOperator(
    task_id='PREPARE_CSV_FILE_PATTERN',
    python_callable=prepare_file_pattern,
    dag=dag,
)

get_context_date_for_csv  # TO DO