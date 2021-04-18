from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

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
    username + '.dwh_etl',
    default_args=default_args,
    description='Data Warehouse ETL tasks',
    schedule_interval="0 0 1 1 *",
)

view_payment_one_year = PostgresOperator(
    task_id="view_payment_one_year",
    dag=dag,
    sql="""
          create or replace view yfurman.view_payment_one_year as (

            with staging as (
              with derived_columns as (
                select
                  user_id,
                  pay_doc_type,
                  pay_doc_num,
                  account,
                  phone,
                  billing_period,
                  pay_date,
                  sum,
                  user_id::varchar as USER_KEY,
                  account::varchar as ACCOUNT_KEY,
                  billing_period::varchar as BILLING_PERIOD_KEY,
                  pay_doc_type::varchar as PAY_DOC_TYPE_KEY,
                  pay_doc_num::varchar as PAY_DOC_NUM_KEY,
                  'PAYMENT - DATA LAKE'::varchar as RECORD_SOURCE
                from yfurman.ods_payment 
                where cast(extract('year' from cast(pay_date as timestamp)) as int) = {{ execution_date.year }}
              ),

              hashed_columns as (
                select
                  user_id,
                  pay_doc_type,
                  pay_doc_num,
                  account,
                  phone,
                  billing_period,
                  pay_date,
                  sum,
                  USER_KEY,
                  ACCOUNT_KEY,
                  BILLING_PERIOD_KEY,
                  PAY_DOC_TYPE_KEY,
                  PAY_DOC_NUM_KEY,
                  RECORD_SOURCE,

                  cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,
                  cast((md5(nullif(upper(trim(cast(account as varchar))), ''))) as TEXT) as ACCOUNT_PK,
                  cast((md5(nullif(upper(trim(cast(billing_period as varchar))), ''))) as TEXT) as BILLING_PERIOD_PK,
                  cast(md5(nullif(concat_ws('||',
                    coalesce(nullif(upper(trim(cast(pay_doc_type as varchar))), ''), '^^'),
                    coalesce(nullif(upper(trim(cast(pay_doc_num as varchar))), ''), '^^')
                  ), '^^||^^')) as TEXT) as PAY_DOC_PK,
                  cast(md5(nullif(concat_ws('||',
                    coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
                    coalesce(nullif(upper(trim(cast(account as varchar))), ''), '^^')
                  ), '^^||^^')) as TEXT) as USER_ACCOUNT_PK,
                  cast(md5(nullif(concat_ws('||',
                    coalesce(nullif(upper(trim(cast(account as varchar))), ''), '^^'),
                    coalesce(nullif(upper(trim(cast(pay_doc_type as varchar))), ''), '^^'),
                    coalesce(nullif(upper(trim(cast(pay_doc_num as varchar))), ''), '^^'),
                    coalesce(nullif(upper(trim(cast(billing_period as varchar))), ''), '^^')
                  ), '^^||^^||^^||^^')) as TEXT) as ACCOUNT_BILLING_PAY_PK,
                  cast(md5(concat_ws('||',
                    coalesce(nullif(upper(trim(cast(phone as varchar))), ''), '^^')
                  )) as TEXT) as USER_HASHDIFF,
                  cast(md5(concat_ws('||',
                    coalesce(nullif(upper(trim(cast(pay_date as varchar))), ''), '^^'),
                    coalesce(nullif(upper(trim(cast(sum as varchar))), ''), '^^')
                  )) as TEXT) as PAY_DOC_HASHDIFF
                from derived_columns
              ),

              columns_to_select as (
                select
                  user_id,
                  pay_doc_type,
                  pay_doc_num,
                  account,
                  phone,
                  billing_period,
                  pay_date,
                  sum,
                  USER_KEY,
                  ACCOUNT_KEY,
                  BILLING_PERIOD_KEY,
                  PAY_DOC_TYPE_KEY,
                  PAY_DOC_NUM_KEY,
                  RECORD_SOURCE,
                  USER_PK,
                  ACCOUNT_PK,
                  BILLING_PERIOD_PK,
                  PAY_DOC_PK,
                  USER_ACCOUNT_PK,
                  ACCOUNT_BILLING_PAY_PK,
                  USER_HASHDIFF,
                  PAY_DOC_HASHDIFF
                from hashed_columns
              )

              select * from columns_to_select
            )

            select *, 
                current_timestamp as LOAD_DATE,
                pay_date as EFFECTIVE_FROM
            from staging
          );
    """
)

for phase in ('HUB', 'LINK', 'SATELLITE'):
    # Load HUBs
    if phase == 'HUB':
          tasks = ('HUB_USER', 'HUB_ACCOUNT', 'HUB_BILLING_PERIOD', 'HUB_PAY_DOC')
          hubs = []
          for task in tasks:
              if task == 'HUB_USER':
                  query = """
                          with row_rank_1 as (
                              select * from (
                                select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
                                  row_number() over (
                                    partition by USER_PK
                                    order by LOAD_DATE ASC
                                  ) as row_num
                                from yfurman.view_payment_one_year  		
                              ) as h where row_num = 1
                            ),	
                          records_to_insert as (
                              select a.USER_PK, a.USER_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                              from row_rank_1 as a
                              left join yfurman.dds_hub_user as d
                              on a.USER_PK = d.USER_PK
                              where d.USER_PK is NULL
                          )
                          insert into yfurman.dds_hub_user (USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE)
                          (
                            select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE
                            from records_to_insert
                          );
                      """
              elif task == 'HUB_ACCOUNT':
                  query = """
                          with row_rank_1 as (
                              select * from (
                                select ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
                                  row_number() over (
                                    partition by ACCOUNT_PK
                                    order by LOAD_DATE ASC
                                  ) as row_num
                                from yfurman.view_payment_one_year  		
                              ) as h where row_num = 1
                            ),	
                          records_to_insert as (
                              select a.ACCOUNT_PK, a.ACCOUNT_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                              from row_rank_1 as a
                              left join yfurman.dds_hub_account as d
                              on a.ACCOUNT_PK = d.ACCOUNT_PK
                              where d.ACCOUNT_PK is NULL
                          )
                          insert into yfurman.dds_hub_account (ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE)
                          (
                            select ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE
                            from records_to_insert
                          );
                      """
              elif task == 'HUB_BILLING_PERIOD':
                  query = """
                          with row_rank_1 as (
                              select * from (
                                select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
                                  row_number() over (
                                    partition by BILLING_PERIOD_PK
                                    order by LOAD_DATE ASC
                                  ) as row_num
                                from yfurman.view_payment_one_year  		
                              ) as h where row_num = 1
                            ),	
                          records_to_insert as (
                              select a.BILLING_PERIOD_PK, a.BILLING_PERIOD_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                              from row_rank_1 as a
                              left join yfurman.dds_hub_billing_period as d
                              on a.BILLING_PERIOD_PK = d.BILLING_PERIOD_PK
                              where d.BILLING_PERIOD_PK is NULL
                          )
                          insert into yfurman.dds_hub_billing_period (BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE)
                          (
                            select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE
                            from records_to_insert
                          );
                      """
              elif task == 'HUB_PAY_DOC':
                  query = """
                          with row_rank_1 as (
                              select * from (
                                select PAY_DOC_PK, PAY_DOC_TYPE_KEY, PAY_DOC_NUM_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
                                  row_number() over (
                                    partition by PAY_DOC_PK
                                    order by LOAD_DATE ASC
                                  ) as row_num
                                from yfurman.view_payment_one_year  		
                              ) as h where row_num = 1
                            ),	
                          records_to_insert as (
                              select a.PAY_DOC_PK, a.PAY_DOC_TYPE_KEY, a.PAY_DOC_NUM_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                              from row_rank_1 as a
                              left join yfurman.dds_hub_pay_doc as d
                              on a.PAY_DOC_PK = d.PAY_DOC_PK
                              where d.PAY_DOC_PK is NULL
                          )
                          insert into yfurman.dds_hub_pay_doc (PAY_DOC_PK, PAY_DOC_TYPE_KEY, PAY_DOC_NUM_KEY, LOAD_DATE, RECORD_SOURCE)
                          (
                            select PAY_DOC_PK, PAY_DOC_TYPE_KEY, PAY_DOC_NUM_KEY, LOAD_DATE, RECORD_SOURCE
                            from records_to_insert
                          );
                      """
              dds.append(DataProcHiveOperator(
                  task_id='dds_' + task,
                  dag=dag,
                  sql=sql
              ))
          all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
    
    # Load LINKs
    if phase == 'LINK':
          tasks = ('LINK_USER_ACCOUNT', 'LINK_ACCOUNT_BILLING_PAY')
          links = []
          for task in tasks:
              if task == 'LINK_USER_ACCOUNT':
                  query = """
                          with records_to_insert as (
                              select distinct 
                                  stg.USER_ACCOUNT_PK, 
                                  stg.USER_PK, stg.ACCOUNT_PK, 
                                  stg.LOAD_DATE, stg.RECORD_SOURCE
                              from yfurman.view_payment_one_year as stg 
                              left join yfurman.dds_link_user_account as tgt
                              on stg.USER_ACCOUNT_PK = tgt.USER_ACCOUNT_PK
                              where tgt.USER_ACCOUNT_PK is null		
                          )
                          insert into yfurman.dds_link_user_account (
                              USER_ACCOUNT_PK,
                              USER_PK, ACCOUNT_PK,
                              LOAD_DATE, RECORD_SOURCE)
                          (
                              select 
                                  USER_ACCOUNT_PK,
                                  USER_PK, ACCOUNT_PK,
                                  LOAD_DATE, RECORD_SOURCE
                              from records_to_insert
                          );
                      """
              elif task == 'LINK_ACCOUNT_BILLING_PAY':
                  query = """
                          with records_to_insert as (
                              select distinct 
                                  stg.ACCOUNT_BILLING_PAY_PK, 
                                  stg.ACCOUNT_PK, stg.BILLING_PERIOD_PK, stg.PAY_DOC_PK, 
                                  stg.LOAD_DATE, stg.RECORD_SOURCE
                              from yfurman.view_payment_one_year as stg 
                              left join yfurman.dds_link_account_billing_pay as tgt
                              on stg.ACCOUNT_BILLING_PAY_PK = tgt.ACCOUNT_BILLING_PAY_PK
                              where tgt.ACCOUNT_BILLING_PAY_PK is null		
                          )
                          insert into yfurman.dds_link_account_billing_pay (
                              ACCOUNT_BILLING_PAY_PK,
                              ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_PK,
                              LOAD_DATE, RECORD_SOURCE)
                          (
                              select 
                                  ACCOUNT_BILLING_PAY_PK,
                                  ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_PK,
                                  LOAD_DATE, RECORD_SOURCE
                              from records_to_insert
                          );
                      """
                     
              dds.append(DataProcHiveOperator(
                  task_id='dds_' + task,
                  dag=dag,
                  sql=sql
              ))
          all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)


