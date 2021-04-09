import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def log_details(*args, **kwargs):
    # NOTE: Look here for context variables passed in on kwargs:
    #       https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
    #
    logging.info(f"Execution date is {kwargs['ds']}")
    logging.info(f"My run id is {kwargs['run_id']}")

    previous_ds = kwargs.get('previous_ds')
    next_ds = kwargs.get('next_ds')
    if previous_ds:
        logging.info(f"My previous run was on {previous_ds}")
    if next_ds:
        logging.info(f"My next run will be {next_ds}")


dag = DAG(
    'context_and_templating',
    schedule_interval="@daily",
    start_date=datetime.datetime.now() - datetime.timedelta(days=2)
)

list_task = PythonOperator(
    task_id="log_details",
    python_callable=log_details,
    provide_context=True,
    dag=dag
)
