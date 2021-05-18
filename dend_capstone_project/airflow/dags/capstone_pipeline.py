from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from load_to_redshift import LoadToRedshiftOperator


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

# trip_data_etl = StageToRedshiftOperator(
#     task_id='stage_events',
#     dag=dag,
#     aws_credentials_id="aws_credentials",
#     redshift_conn_id="redshift",
#     table="staging_events",
#     s3_path="s3://udacity-dend/log_data",
#     json_path="s3://udacity-dend/log_json_path.json")
#
# covid_data_etl = StageToRedshiftOperator(
#     task_id='stage_songs',
#     dag=dag,
#     aws_credentials_id="aws_credentials",
#     redshift_conn_id="redshift",
#     table="staging_songs",
#     s3_path="s3://udacity-dend/song_data"
# )
#
# weather_data_etl = StageToRedshiftOperator(
#     task_id='stage_songs',
#     dag=dag,
#     aws_credentials_id="aws_credentials",
#     redshift_conn_id="redshift",
#     table="staging_songs",
#     s3_path="s3://udacity-dend/song_data"
# )

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

# run_quality_checks = DataQualityOperator(
#     task_id='run_data_quality_checks',
#     dag=dag,
#     redshift_conn_id="redshift",
#     tables=['songs', 'time', 'users', 'artists', 'songplays']
# )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

[load_trip_table,
 load_station_table,
 load_covid_table,
 load_weather_table] >> end_operator
