from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 2, 23, 11, 35, 0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
}

dag = DAG(
    'git_sync',
    default_args=default_args,
    description='A simple tutorial DAG',
    catchup=False,
    schedule_interval=timedelta(seconds=60),
)

latest_only = LatestOnlyOperator(task_id='latest_only', dag=dag)

git_pull = BashOperator(
    task_id='git_pull',
    bash_command='git -C /root/airflow/dags/ pull',
    dag=dag,
)

latest_only >> git_pull
