from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 30),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('long_running_queries_alert', default_args=default_args, schedule_interval='@daily')

# Snowflake connection ID defined in Airflow
snowflake_conn_id = 'ex_alert_snowflake'

# Task to check for long-running queries and send email if any
def check_and_send_email(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    # SQL query to retrieve long-running queries
    long_running_queries_query = """
    SELECT query_id, query_text, start_time, end_time, DATEDIFF('SECOND', start_time, end_time) AS duration
    FROM table(information_schema.query_history())
    WHERE DATEDIFF('SECOND', start_time, end_time) > 300
    ORDER BY duration DESC
    """

    # Execute the query using the SnowflakeHook
    query_results = snowflake_hook.get_records(sql=long_running_queries_query)
    logging.info(query_results)
    # Prepare email content
    if query_results:
        email_subject = "Long Running Queries Alert"
        email_body = "The following queries have been running for more than 5 minutes:\n\n"
        for row in query_results:
            query_id, query_text, start_time, end_time, duration = row
            email_body += f"Query ID: {query_id}, Duration: {duration} seconds\nQuery Text: {query_text}\n\n"

        # Send email
        recipients = "samiksha.pandit@numerator.com" # Variable.get("email_recipients", default_var="")
        email_operator = EmailOperator(
            task_id='send_email_alert',
            to=recipients,
            subject=email_subject,
            html_content=email_body,
            dag=dag
        )
        email_operator.execute(context=kwargs)

# Task to check and send email
check_and_send_email_task = PythonOperator(
    task_id='check_and_send_email',
    python_callable=check_and_send_email,
    provide_context=True,
    dag=dag
)

check_and_send_email_task
