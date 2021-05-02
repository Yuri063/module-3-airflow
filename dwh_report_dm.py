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
            'HUB_BILLING_PERIOD':  """
                    with row_rank_1 as (
                        select * from (
                          select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
                            row_number() over (
                              partition by BILLING_PERIOD_PK
                              order by LOAD_DATE ASC
                            ) as row_num
                          from yfurman.view_payment_{{ execution_date.year }}  		
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
            """,
            'HUB_PAY_DOC':  """
                  with row_rank_1 as (
                      select * from (
                        select PAY_DOC_PK, PAY_DOC_TYPE_KEY, PAY_DOC_NUM_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
                          row_number() over (
                            partition by PAY_DOC_PK
                            order by LOAD_DATE ASC
                          ) as row_num
                        from yfurman.view_payment_{{ execution_date.year }}  		
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
            """},
    'LINKS': {
            'LINK_USER_ACCOUNT_BILLING_PAY':  """
                      with records_to_insert as (
                          select distinct 
                              stg.USER_ACCOUNT_BILLING_PAY_PK, 
                              stg.USER_PK, stg.ACCOUNT_PK, stg.BILLING_PERIOD_PK, stg.PAY_DOC_PK, 
                              stg.LOAD_DATE, stg.RECORD_SOURCE
                          from yfurman.view_payment_{{ execution_date.year }} as stg 
                          left join yfurman.dds_link_user_account_billing_pay as tgt
                          on stg.USER_ACCOUNT_BILLING_PAY_PK = tgt.USER_ACCOUNT_BILLING_PAY_PK
                          where tgt.USER_ACCOUNT_BILLING_PAY_PK is null		
                      )
                      insert into yfurman.dds_link_user_account_billing_pay (
                          USER_ACCOUNT_BILLING_PAY_PK,
                          USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_PK,
                          LOAD_DATE, RECORD_SOURCE)
                      (
                          select 
                              USER_ACCOUNT_BILLING_PAY_PK,
                              USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_PK,
                              LOAD_DATE, RECORD_SOURCE
                          from records_to_insert
                      );
            """},
    'SATELLITES': {
            'SAT_USER_DETAILS':  """
                    with source_data as (
                        select 
                            USER_PK, USER_HASHDIFF, 
                            phone, 
                            EFFECTIVE_FROM, 
                            LOAD_DATE, RECORD_SOURCE
                        from yfurman.view_payment_{{ execution_date.year }}
                    ),                  
                    update_records as (
                        select 
                            a.USER_PK, a.USER_HASHDIFF, 
                            a.phone, 
                            a.EFFECTIVE_FROM, 
                            a.LOAD_DATE, a.RECORD_SOURCE
                        from yfurman.dds_sat_user_details as a
                        join source_data as b
                        on a.USER_PK = b.USER_PK
                        where  a.LOAD_DATE <= b.LOAD_DATE
                    ),
                    latest_records as (
                        select * from (
                            select USER_PK, USER_HASHDIFF, LOAD_DATE,
                                case when rank() over (partition by USER_PK order by LOAD_DATE desc) = 1
                                    then 'Y' 
                                    else 'N'
                                end as latest
                            from update_records
                        ) as s
                        where latest = 'Y'
                    ),	
                    records_to_insert as (
                        select distinct 
                            e.USER_PK, e.USER_HASHDIFF, 
                            e.phone, 
                            e.EFFECTIVE_FROM, 
                            e.LOAD_DATE, e.RECORD_SOURCE
                        from source_data as e
                        left join latest_records
                        on latest_records.USER_HASHDIFF = e.USER_HASHDIFF and
                           latest_records.USER_PK = e.USER_PK
                        where latest_records.USER_HASHDIFF is NULL
                    )	
                    insert into yfurman.dds_sat_user_details (
                        USER_PK, USER_HASHDIFF, 
                        phone, 
                        EFFECTIVE_FROM, 
                        LOAD_DATE, RECORD_SOURCE)
                    (
                        select 
                            USER_PK, USER_HASHDIFF, 
                            phone, 
                            EFFECTIVE_FROM, 
                            LOAD_DATE, RECORD_SOURCE
                        from records_to_insert
                    );                  
            """,
            'SAT_PAY_DETAILS':  """
                    with source_data as (
                        select 
                            USER_ACCOUNT_BILLING_PAY_PK, PAY_DOC_HASHDIFF, 
                            pay_date, sum, 
                            EFFECTIVE_FROM, 
                            LOAD_DATE, RECORD_SOURCE
                        from yfurman.view_payment_{{ execution_date.year }}                            
                    ),
                    update_records as (
                        select 
                            a.USER_ACCOUNT_BILLING_PAY_PK, a.PAY_DOC_HASHDIFF, 
                            a.pay_date, a.sum,
                            a.EFFECTIVE_FROM, 
                            a.LOAD_DATE, a.RECORD_SOURCE
                        from yfurman.dds_sat_pay_details as a
                        join source_data as b
                        on a.USER_ACCOUNT_BILLING_PAY_PK = b.USER_ACCOUNT_BILLING_PAY_PK
                        where  a.LOAD_DATE <= b.LOAD_DATE
                    ),
                    latest_records as (
                        select * from (
                            select USER_ACCOUNT_BILLING_PAY_PK, PAY_DOC_HASHDIFF, LOAD_DATE,
                                case when rank() over (partition by USER_ACCOUNT_BILLING_PAY_PK order by LOAD_DATE desc) = 1
                                    then 'Y' 
                                    else 'N'
                                end as latest
                            from update_records
                        ) as s
                        where latest = 'Y'
                    ),	
                    records_to_insert as (
                        select distinct 
                            e.USER_ACCOUNT_BILLING_PAY_PK, e.PAY_DOC_HASHDIFF, 
                            e.pay_date, e.sum,
                            e.EFFECTIVE_FROM, 
                            e.LOAD_DATE, e.RECORD_SOURCE
                        from source_data as e
                        left join latest_records
                        on latest_records.PAY_DOC_HASHDIFF = e.PAY_DOC_HASHDIFF and
                           latest_records.USER_ACCOUNT_BILLING_PAY_PK = e.USER_ACCOUNT_BILLING_PAY_PK
                        where latest_records.PAY_DOC_HASHDIFF is NULL
                    )	
                    insert into yfurman.dds_sat_pay_details (
                        USER_ACCOUNT_BILLING_PAY_PK, PAY_DOC_HASHDIFF, 
                        pay_date, sum, 
                        EFFECTIVE_FROM, 
                        LOAD_DATE, RECORD_SOURCE)
                    (
                        select 
                            USER_ACCOUNT_BILLING_PAY_PK, PAY_DOC_HASHDIFF, 
                            pay_date, sum, 
                            EFFECTIVE_FROM, 
                            LOAD_DATE, RECORD_SOURCE
                        from records_to_insert
                    );                  
            """},
    'DROP_VIEW_PAYMENT_ONE_YEAR': """
          drop view if exists yfurman.view_payment_{{ execution_date.year }};
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

for phase in ('DIMENSIONS', 'FACTS'):
    # Load DIMENSIONs    
    if phase == 'DIMENSIONs':
        dims = get_phase_context(phase)
        all_dims_loaded = DummyOperator(task_id="all_dims_loaded", dag=dag)

    # Load FACTs
    elif phase == 'FACTS':
        facts = get_phase_context(phase)
        all_facts_loaded = DummyOperator(task_id="all_facts_loaded", dag=dag)


drop_payment_report_tmp_one_year = PostgresOperator(
    task_id='DROP_PAYMENT_REPORT_TMP_ONE_YEAR',
    dag=dag,
    sql=SQL_CONTEXT['DROP_PAYMENT_REPORT_TMP_ONE_YEAR']
)

load_payment_report_tmp_one_year >> dims >> all_dims_loaded >> facts >> all_facts_loaded >>  drop_payment_report_tmp_one_year
