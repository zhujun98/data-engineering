from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from load_to_redshift import LoadToRedshiftOperator
from data_quality import DataCountCheckOperator, TripDateCheckOperator


default_args = {
    'owner': 'capstone',
    'depends_on_past': False,  # DAG does not have dependencies on past runs
    'start_date': datetime(2021, 5, 12),
    'retries': 3,  # Task will be retried for 3 time in case of failure
    'retry_delay': timedelta(minutes=5),  # Interval between retries is 5 min
    'catchup': False,  # Only run latest
    'email_on_retry': False  # Do not email on retry
}

dag = DAG('capstone_pipeline',
          default_args=default_args,
          max_active_runs=1)

S3_BUCKET = "s3://dend-capstone-project-workspace/processed/"

load_trip_table = LoadToRedshiftOperator(
    task_id='load_trip_table_to_redshift',
    dag=dag,
    table="trip",
    s3_path=S3_BUCKET + "trip_data"
)

load_station_table = LoadToRedshiftOperator(
    task_id='load_station_table_to_redshift',
    dag=dag,
    table="station",
    s3_path=S3_BUCKET + "station_data"
)

load_covid_table = LoadToRedshiftOperator(
    task_id='load_covid_table_to_redshift',
    dag=dag,
    table="covid",
    s3_path=S3_BUCKET + "covid_data"
)

load_weather_table = LoadToRedshiftOperator(
    task_id='load_weather_table_to_redshift',
    dag=dag,
    table="weather",
    s3_path=S3_BUCKET + "weather_data"
)

data_count_checks = DataCountCheckOperator(
    task_id='check_data_count',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['trip', 'station', 'covid', 'weather']
)

date_checks = TripDateCheckOperator(
    task_id='check_trip_date',
    dag=dag,
    redshift_conn_id="redshift"
)

finish_load_operator = DummyOperator(task_id='finish_loading',  dag=dag)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

[load_trip_table, load_station_table, load_covid_table, load_weather_table] \
    >> finish_load_operator \
    >> [data_count_checks, date_checks] \
    >> end_operator
