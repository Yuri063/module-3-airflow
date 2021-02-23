from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("spacex_v01", default_args=default_args, schedule_interval="0 0 1 1 *")

for rocket in ('falcon1', 'falcon9', 'falconheavy', 'all'):
    params = {"rocket": rocket} # falcon1/falcon9/falconheavy/all
    t1 = BashOperator(
        task_id="get_data." + rocket,         
        bash_command="python3 /root/airflow/dags/spacex/load_launches.py -y {{{{ execution_date.year }}}} -o /var/data{}".format(" -r {{ params.rocket }}" if rocket !='all' else ""),        
        params=params,
        dag=dag
    )
    
    t2 = BashOperator(
        task_id="print_data." + rocket, 
        bash_command="cat /var/data/year={{ execution_date.year }}/rocket={{ params.rocket }}/data.csv", 
        params=params,
        dag=dag
    )
    t1 >> t2

