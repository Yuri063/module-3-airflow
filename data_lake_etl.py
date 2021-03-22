from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2012, 1, 1, 0, 0, 0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
}

dag = DAG(
    'data_lake_etl',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_billing = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query="""
        insert overwrite table ods.billing partition (year='{{ execution_date.year }}') 
        select * from stg.billing where year(from_unixtime(`timestamp` div 1000)) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    region='us-central1',
)
