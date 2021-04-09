# Scheduling, partitioning and data quality check
import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements


PARTITION_BY_MONTH = True


def load_trip_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials", client_type="s3")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    if PARTITION_BY_MONTH:
        execution_date = kwargs.get('execution_date')
        sql_stmt = sql_statements.COPY_MONTHLY_TRIPS_SQL.format(
            credentials.access_key,
            credentials.secret_key,
            year=execution_date.year,
            month=execution_date.month
        )
    else:
        sql_stmt = sql_statements.COPY_ALL_TRIPS_SQL.format(
            credentials.access_key,
            credentials.secret_key
        )
    redshift_hook.run(sql_stmt)


def load_station_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials", client_type="s3")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.COPY_STATIONS_SQL.format(
        credentials.access_key,
        credentials.secret_key
    )
    redshift_hook.run(sql_stmt)


def check_greater_than_zero(*args, **kwargs):
    table = kwargs["params"]["table"]
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Data quality check failed. {table} returned no results")
    num_records = records[0][0]
    if num_records < 1:
        raise ValueError(f"Data quality check failed. {table} contained 0 rows")

    logging.info(f"Data quality on table {table} check passed with "
                 f"{records[0][0]} records")


dag = DAG(
    'bicycle_sharing_example',
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2018, 12, 1, 0, 0, 0, 0),
    schedule_interval='@monthly',
    max_active_runs=1
)

create_trips_table = PostgresOperator(
    task_id="create_trips_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TRIPS_TABLE_SQL
)

# https://airflow.apache.org/docs/apache-airflow/stable/_modules/airflow/models/baseoperator.html
copy_trips_task = PythonOperator(
    task_id='load_trips_from_s3_to_redshift',
    dag=dag,
    python_callable=load_trip_data_to_redshift,
    provide_context=True,
    sla=datetime.timedelta(hours=1)
)

check_trips = PythonOperator(
    task_id='check_trips_data',
    dag=dag,
    python_callable=check_greater_than_zero,
    provide_context=True,
    params={
        'table': 'trips',
    }
)

create_stations_table = PostgresOperator(
    task_id="create_stations_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
)

copy_stations_task = PythonOperator(
    task_id='load_stations_from_s3_to_redshift',
    dag=dag,
    python_callable=load_station_data_to_redshift,
)

check_stations = PythonOperator(
    task_id='check_stations_data',
    dag=dag,
    python_callable=check_greater_than_zero,
    provide_context=True,
    params={
        'table': 'stations',
    }
)

create_trips_table >> copy_trips_task
copy_trips_task >> check_trips

create_stations_table >> copy_stations_task
copy_stations_task >> check_stations
