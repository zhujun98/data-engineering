# Scheduling, partitioning and data quality check
import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from s3_to_redshift import S3ToRedshiftOperator
from facts_calculator import FactsCalculatorOperator
from has_rows import HasRowsOperator

import sql_statements


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
copy_trips_task = S3ToRedshiftOperator(
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="trips",
    s3_path="s3://udacity-dend/data-pipelines/divvy/partitioned/{execution_date.year}/{execution_date.month}/divvy_trips.csv",
    task_id='load_trips_from_s3_to_redshift',
    dag=dag,
    sla=datetime.timedelta(hours=1)
)

check_trips = HasRowsOperator(
    task_id='check_trips_data',
    dag=dag,
    redshift_conn_id="redshift",
    table='trips'
)

calculate_facts = FactsCalculatorOperator(
    task_id="calculate_facts_trips",
    dag=dag,
    redshift_conn_id="redshift",
    origin_table="trips",
    destination_table="trips_facts",
    fact_column="tripduration",
    groupby_column="bikeid"
)

create_stations_table = PostgresOperator(
    task_id="create_stations_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
)

copy_stations_task = S3ToRedshiftOperator(
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="stations",
    s3_path="s3://udacity-dend/data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
    task_id='load_stations_from_s3_to_redshift',
    dag=dag
)

check_stations = HasRowsOperator(
    task_id='check_stations_data',
    dag=dag,
    redshift_conn_id="redshift",
    table='stations'
)


create_trips_table >> copy_trips_task
copy_trips_task >> check_trips
check_trips >> calculate_facts

create_stations_table >> copy_stations_task
copy_stations_task >> check_stations
