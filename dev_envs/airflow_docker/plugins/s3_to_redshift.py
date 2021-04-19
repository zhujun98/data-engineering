from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    _COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER 1
        DELIMITER ','
    """
    @apply_defaults
    def __init__(self, *,
                 aws_credentials_id: str,
                 redshift_conn_id: str,
                 table: str,
                 s3_path: str,
                 **kwargs):
        super().__init__(**kwargs)

        self._aws_credentials_id = aws_credentials_id
        self._redshift_conn_id = redshift_conn_id
        self._table = table
        self._s3_path = s3_path

    def execute(self, context):
        """Override."""
        aws_hook = AwsHook(self._aws_credentials_id, client_type="s3")
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self._redshift_conn_id)

        s3_path = self._s3_path.format(**context)

        sql_stmt = self._COPY_SQL.format(
            self._table,
            s3_path,
            credentials.access_key,
            credentials.secret_key
        )
        redshift_hook.run(sql_stmt)
