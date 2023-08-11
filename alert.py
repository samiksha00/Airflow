from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
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
    WHERE DATEDIFF('SECOND', start_time, end_time) > 1
    ORDER BY duration DESC
    """

    # Execute the query using the SnowflakeHook
    query_results = snowflake_hook.get_records(sql=long_running_queries_query)

    # If query results, send email
    if query_results and query_results[0]:
        email_subject = "Long Running Queries Alert"
        email_body = "The following queries have been running for more than 5 minutes:\n\n"
        for record in query_results[0]:  # Iterate through all records
            query_id = record['QUERY_ID']
            query_text = record['QUERY_TEXT']

            start_time = record['START_TIME']
            formatted_start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')  # Format as desired

            end_time = record['END_TIME']
            formatted_end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')  # Format as desired

            duration = record['DURATION']

            email_body += f"Query ID: {query_id}, Duration: {duration} seconds\n"
            email_body += f"Query Text: {query_text}\n"
            email_body += f"Start Time: {formatted_start_time}\n"
            email_body += f"End Time: {formatted_end_time}\n\n"

        # Send email
        # recipients = Variable.get("email_recipients", default_var="")
        recipients = 'samiksha.pandit@numerator.com'
        email_operator = EmailOperator(
            task_id='send_email_alert',
            to=recipients,
            subject=email_subject,
            html_content=email_body,
            dag=dag
        )
        email_operator.execute(context=kwargs)
        # email_operator = EmptyOperator(
        #     task_id='email_operator',
        #     dag=dag
        # )
        return 'send_email_alert'
    else:
        return 'no_query_results'

# Task to check and send email
check_and_send_email_task = PythonOperator(
    task_id='check_and_send_email',
    python_callable=check_and_send_email,
    provide_context=True,
    dag=dag
)

# DummyOperator as a placeholder if no query results
no_query_results_task = EmptyOperator(
    task_id='no_query_results',
    dag=dag
)

# Define the task dependencies
check_and_send_email_task >> [no_query_results_task]
