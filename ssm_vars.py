from airflow.models import Variable
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from nmr_airflow_v2_modules.plugins.dags.dag_factory import DAGFactory


source_name = "xyz"

li="""ALTER TABLE INFOSCOUT_AIRBYTE_DEV.MAESTRO.UN_CORE_ASSIGNMENTACTION ADD COLUMN DATA_TEMP Variant;
UPDATE INFOSCOUT_AIRBYTE_DEV.MAESTRO.UN_CORE_ASSIGNMENTACTION SET DATA_TEMP = parse_json(DATA);
ALTER TABLE INFOSCOUT_AIRBYTE_DEV.MAESTRO.UN_CORE_ASSIGNMENTACTION DROP COLUMN DATA;
ALTER TABLE INFOSCOUT_AIRBYTE_DEV.MAESTRO.UN_CORE_ASSIGNMENTACTION RENAME COLUMN DATA_TEMP to DATA;
"""

str_query = Variable.get(f"airbyte_{source_name}_get_after_clone_sql", default_var=None)

DAG_ID=f'airbyte_{source_name}_get_after_clone_sql'

snowflake_conn_id = 'isc_snowflake'

DEFAULT_ARGS = {
    "start_date": datetime(2023, 9, 20),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
dag = DAGFactory(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    file_paths=["dags/data_log/sql"],
    slack_conn_id=None,
    schedule=None,
)


# def print_q():
#     for q in queries:
#         logging.info(' abc /n')
#         q=q+';'
#         logging.info(q)


with dag:
    # print_q = PythonOperator(
    #         task_id="print_q",
    #         python_callable=print_q,
    #         dag=dag,
    #     )
    if str_query==None:
        get_queries_task = EmptyOperator(task_id="none",dag=dag)
        # print_q >> none
    else:
        queries= str_query.split(';')
        i=1
        # print_q
        for q in queries:
            q=q+';'
            get_queries_task = SnowflakeOperator(
                task_id=f'get_queries_task_{i}',
                sql=q,
                snowflake_conn_id=snowflake_conn_id,
                dag=dag
            )
            i=i+1
        get_queries_task
