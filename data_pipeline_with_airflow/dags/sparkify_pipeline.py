from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from stage_redshift import StageToRedshiftOperator
from load_fact import LoadFactOperator
from load_dimension import LoadDimensionOperator
from data_quality import DataQualityOperator

from helpers.sql_queries import SqlQueries as sqs


default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,  # DAG does not have dependencies on past runs
    'start_date': datetime(2019, 1, 12),
    'retries': 3,  # Task will be retried for 3 time in case of failure
    'retry_delay': timedelta(minutes=5),  # Interval between retries is 5 min
    'catchup': False,  # Only run latest
    'email_on_retry': False  # Do not email on retry
}

dag = DAG('sparkify_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_events",
    s3_path="s3://udacity-dend/log_data",
    json_path="s3://udacity-dend/log_json_path.json")

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_songs",
    s3_path="s3://udacity-dend/song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query_statement=sqs.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query_statement=sqs.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query_statement=sqs.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query_statement=sqs.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query_statement=sqs.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['songs', 'time', 'users', 'artists', 'songplays']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [
    load_song_dimension_table,
    load_time_dimension_table,
    load_user_dimension_table,
    load_artist_dimension_table
] >> run_quality_checks

run_quality_checks >> end_operator
