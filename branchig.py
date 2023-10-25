from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from nmr_airflow_v2_modules.plugins.dags.dag_factory import DAGFactory

source_name = "maestro"

queries = Variable.get(f"airbyte_{source_name}_get_after_clone_sql", default_var=None)
DAG_ID=f'airbyte_{source_name}_get_after_clone_sql'

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

with dag:
    def random_branch():
        return "none" if queries == None else "yes"

    t2 = BranchPythonOperator(task_id="t2", python_callable=random_branch)
    none = EmptyOperator(task_id="none",dag=dag)
    yes=EmptyOperator(task_id="yes",dag=dag)

    t2 >> none
    t2 >> yes
