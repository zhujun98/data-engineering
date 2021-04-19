# Airflow Setup with Docker

For more details, check https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html.

An AWS Redshift cluster is required for DAGs 
[aws_s3_to_redshift](./dags/aws_s3_to_redshift.py) and
[bicycle_sharing_example](./dags/bicycle_sharing_example.py). One can
simply make use of the script in 
[data_warehouse_with_redshift](../../data_warehouse_with_redshift) to
start a Redshift cluster.