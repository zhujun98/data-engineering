from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator,
    DataQualityOperator
)


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
          schedule_interval='@hourly')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag
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
