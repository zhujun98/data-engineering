from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    # 1. Empty the staging table first. Otherwise its size will continue
    #    to grow!
    _SQL_TEMPLATE = ("""
        TRUNCATE {};
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS PARQUET
    """)

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="aws_credentials",
                 redshift_conn_id="redshift",
                 table="",
                 s3_path="",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._aws_credentials_id = aws_credentials_id
        self._redshift_conn_id = redshift_conn_id

        self._table = table
        self._s3_path = s3_path

    def execute(self, context):
        aws_hook = AwsHook(self._aws_credentials_id, client_type="s3")
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self._redshift_conn_id)

        self.log.info(f'Copying data from {self._s3_path} to {self._table}')

        sql_stmt = self._SQL_TEMPLATE.format(
            self._table,
            self._table,
            self._s3_path,
            credentials.access_key,
            credentials.secret_key
        )
        redshift_hook.run(sql_stmt)

        self.log.info(f'Data copied from {self._s3_path} to {self._table}')
