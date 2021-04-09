import datetime

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_statements as ss


# Open Admin->Connections
# - Click "Create"
# - Set "Conn Id" to "redshift", "Conn Type" to "Postgres"
# - Set "Host" based on the Redshift cluster endpoint
# - Set "Schema" to the database name used when creating Redshift cluster
# - Set "Login" and "Password"
# - Set "Port" to 5439

def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials", client_type="s3")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(ss.COPY_ALL_TRIPS_SQL.format(credentials.access_key,
                                                   credentials.secret_key))


dag = DAG(
    'aws_s3_to_redshift',
    start_date=datetime.datetime.now()
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=ss.CREATE_TRIPS_TABLE_SQL
)

copy_task = PythonOperator(
    task_id='load_from_s3_to_redshift',
    dag=dag,
    python_callable=load_data_to_redshift
)

location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=ss.LOCATION_TRAFFIC_SQL
)

create_table >> copy_task
copy_task >> location_traffic_task