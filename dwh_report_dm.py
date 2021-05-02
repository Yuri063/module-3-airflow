from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

# SQL Scripts definition

SQL_CONTEXT = {
    'LOAD_PAYMENT_REPORT_TMP_ONE_YEAR': """
           create table yfurman.payment_report_tmp_{{ execution_date.year }} as
              with raw_data as (
                select 
                      legal_type,
                      district,
                      extract(year from registered_at) as registration_year,
                      is_vip,
                      extract(year from to_date(BILLING_PERIOD_KEY, 'YYYY-MM')) as billing_year,
                      sum as billing_sum
                from yfurman.dds_link_user_account_billing_pay luabp 
                join yfurman.dds_hub_billing_period hbp on luabp.BILLING_PERIOD_PK = hbp.BILLING_PERIOD_PK
                join yfurman.dds_sat_pay_details spd  on luabp.USER_ACCOUNT_BILLING_PAY_PK = spd.USER_ACCOUNT_BILLING_PAY_PK
                left join yfurman.dds_sat_user_mdm_details sumd on luabp.USER_PK = sumd.USER_PK
                where extract(year from to_date(BILLING_PERIOD_KEY, 'YYYY-MM')) = {{ execution_date.year }}			

              )		
              select billing_year, legal_type, district, registration_year, is_vip, sum(billing_sum)
              from raw_data
              group by billing_year, legal_type, district, registration_year, is_vip
              order by billing_year, legal_type, district, registration_year, is_vip
          ;
    """,
    'DIMENSIONS': {
            'DIM_BILLING_YEAR':  """      
                    insert into yfurman.payment_report_dim_billing_year(billing_year_key)
                    select distinct billing_year as billing_year_key 
                    from yfurman.payment_report_tmp_{{ execution_date.year }} a
                    left join yfurman.payment_report_dim_billing_year b on b.billing_year_key = a.billing_year
                    where b.billing_year_key is null;
            """,
            'DIM_LEGAL_TYPE':  """
                    insert into yfurman.payment_report_dim_legal_type(legal_type_key)
                    select distinct legal_type as legal_type_key 
                    from yfurman.payment_report_tmp_{{ execution_date.year }} a
                    left join yfurman.payment_report_dim_legal_type b on b.legal_type_key = a.legal_type
                    where b.legal_type_key is null;
            """,
            'DIM_DISTRICT':  """
                    insert into yfurman.payment_report_dim_district(district_key)
                    select distinct district as district_key 
                    from yfurman.payment_report_tmp_{{ execution_date.year }} a
                    left join yfurman.payment_report_dim_district b on b.district_key = a.district
                    where b.district_key is null;
            """,
            'DIM_REGISTRATION_YEAR':  """
                    insert into yfurman.payment_report_dim_registration_year(registration_year_key)
                    select distinct registration_year as registration_year_key 
                    from yfurman.payment_report_tmp_{{ execution_date.year }} a
                    left join yfurman.payment_report_dim_registration_year b on b.registration_year_key = a.registration_year
                    where b.registration_year_key is null;
            """},
    'FACTS': {
            'REPORT_FACT':  """
                    insert into yfurman.payment_report_fct(
                                billing_year_id,
                                legal_type_id,
                                district_id,
                                registration_year_id,
                                is_vip,
                                sum 
                            )
                    select biy.id, lt.id, d.id, ry.id, is_vip, raw.sum
                    from yfurman.payment_report_tmp_{{ execution_date.year }} raw
                    join yfurman.payment_report_dim_billing_year biy on raw.billing_year = biy.billing_year_key
                    join yfurman.payment_report_dim_legal_type lt on raw.legal_type = lt.legal_type_key
                    join yfurman.payment_report_dim_district d on raw.district = d.district_key
                    join yfurman.payment_report_dim_registration_year ry on raw.registration_year = ry.registration_year_key; 
            """},
    'DROP_PAYMENT_REPORT_TMP_ONE_YEAR': """
          drop view if exists yfurman.payment_report_tmp_{{ execution_date.year }};
     """
}

def get_phase_context(task_phase):
    tasks = []
    for task in SQL_CONTEXT[task_phase]:
        query = SQL_CONTEXT[task_phase][task]
        tasks.append(PostgresOperator(
            task_id='dds_{}_{}'.format(task_phase, task),
            dag=dag,
            sql=query
        ))
    return tasks

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
    username + '.dwh_report_dm',
    default_args=default_args,
    description='Data Warehouse - Report DataMarts',
    schedule_interval="0 0 1 1 *",
    concurrency=1,
    max_active_runs=1,
)

load_payment_report_tmp_one_year = PostgresOperator(
    task_id='LOAD_PAYMENT_REPORT_TMP_ONE_YEAR',
    dag=dag,
    sql=SQL_CONTEXT['LOAD_PAYMENT_REPORT_TMP_ONE_YEAR']
)
"""
for phase in ('DIMENSIONS', 'FACTS'):
    # Load DIMENSIONs    
    if phase == 'DIMENSIONs':
        dims = get_phase_context(phase)
        all_dims_loaded = DummyOperator(task_id="all_dims_loaded", dag=dag)

    # Load FACTs
    elif phase == 'FACTS':
        facts = get_phase_context(phase) 
        all_facts_loaded = DummyOperator(task_id="all_facts_loaded", dag=dag)
"""
all_dims_loaded = DummyOperator(task_id="all_dims_loaded", dag=dag)
all_facts_loaded = DummyOperator(task_id="all_facts_loaded", dag=dag)

drop_payment_report_tmp_one_year = PostgresOperator(
    task_id='DROP_PAYMENT_REPORT_TMP_ONE_YEAR',
    dag=dag,
    sql=SQL_CONTEXT['DROP_PAYMENT_REPORT_TMP_ONE_YEAR']
)

#load_payment_report_tmp_one_year >> dims >> all_dims_loaded >> facts >> all_facts_loaded >>  drop_payment_report_tmp_one_year

load_payment_report_tmp_one_year >> get_phase_context('DIMENSIONS') >> all_dims_loaded >> get_phase_context('FACTS') >> all_facts_loaded >>  drop_payment_report_tmp_one_year
