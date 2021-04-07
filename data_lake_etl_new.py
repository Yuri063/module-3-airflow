from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator
# test for cron :)
username = 'yfurman'
default_args = {
    'owner': username,
    'depends_on_past': False,
    'start_date': datetime(2013, 1, 1, 0, 0, 0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=120),
}

dag = DAG(
    username + '.data_lake_etl_new',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)
tasks = ('traffic', 'billing', 'issue', 'payment')
ods = []
for task in tasks:
    if task == 'traffic':
        query = """
               insert overwrite table yfurman.ods_traffic partition (year='{{ execution_date.year }}') 
               select user_id, cast(from_unixtime(`timestamp` div 1000) as TIMESTAMP), device_id, device_ip_addr, bytes_sent, bytes_received 
                      from yfurman.stg_traffic where year(from_unixtime(`timestamp` div 1000)) = {{ execution_date.year }};   
            """
    elif task == 'billing':
        query = """
               insert overwrite table yfurman.ods_billing partition (year='{{ execution_date.year }}') 
               select user_id, billing_period, service, tariff, cast(sum as DECIMAL(10,2)), cast(created_at as TIMESTAMP) 
                      from yfurman.stg_billing where year(created_at) = {{ execution_date.year }};   
            """
    elif task == 'issue':
        query = """
               insert overwrite table yfurman.ods_issue partition (year='{{ execution_date.year }}') 
               select cast(user_id as INT), cast(start_time as TIMESTAMP), cast(end_time as TIMESTAMP), title, description, service 
                      from yfurman.stg_issue where year(start_time) = {{ execution_date.year }};   
            """
    elif task == 'payment':
        query = """
               insert overwrite table yfurman.ods_payment partition (year='{{ execution_date.year }}') 
               select user_id, pay_doc_type, pay_doc_num, account, phone, billing_period, cast(pay_date as DATE), cast(sum as DECIMAL(10,2))
                 from yfurman.stg_payment where year(pay_date) = {{ execution_date.year }};   
            """
    ods.append(DataProcHiveOperator(
        task_id='ods_' + task,
        dag=dag,
        query=query,
        cluster_name='cluster-dataproc',
        region='europe-west3-a',
    ))
    
dm = DataProcHiveOperator(
    task_id='dm_traffic',
    dag=dag,
    query="""
               insert overwrite table yfurman.dm_traffic partition (year='{{ execution_date.year }}')  
               select user_id, max(bytes_received), min(bytes_received), round(avg(bytes_received)) as avg_traf
                 from yfurman.ods_traffic where year = {{ execution_date.year }} group by user_id order by avg_traf;   
          """,
    cluster_name='cluster-dataproc',
    region='europe-west3-a',
)
ods >> dm
