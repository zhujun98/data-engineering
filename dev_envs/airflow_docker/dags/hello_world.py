import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello world!")


dag = DAG('hello_world',
          start_date=datetime.datetime.now() - datetime.timedelta(days=3),
          schedule_interval='@daily')

greet_task = PythonOperator(
    task_id="hello_world_task",
    python_callable=hello_world,
    dag=dag
)
