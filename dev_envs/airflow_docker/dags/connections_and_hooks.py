import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

#
# Open your browser to localhost:8080 and open Admin->Variables
# - Click "Create"
# - Set "Key" equal to "s3_bucket" and set "Val" equal to "udacity-dend"
# - Set "Key" equal to "s3_prefix" and set "Val" equal to "data-pipelines"

# Open Admin->Connections
# - Click "Create"
# - Set "Conn Id" to "aws_credentials", "Conn Type" to "Amazon Web Services"
# - Set "Login" to your aws_access_key_id and "Password" to your
#   aws_secret_key (they can typically be found in ~/.aws/credentials)


def list_keys():
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = Variable.get('s3_bucket')
    prefix = Variable.get('s3_prefix')
    logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket, prefix=prefix)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")


dag = DAG("connections_and_hooks", start_date=datetime.datetime.now())

list_task = PythonOperator(
    task_id="list_keys",
    python_callable=list_keys,
    dag=dag
)
