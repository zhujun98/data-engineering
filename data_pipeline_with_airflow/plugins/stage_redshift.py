from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    # 1. Empty the staging table first. Otherwise its size will continue
    #    to grow!
    # 2. By specifying a JSONPATH, the column names in the source files and
    #    the target table can be different.
    _SQL_TEMPLATE = ("""
        TRUNCATE {};
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
    """)

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_path="",
                 json_path="auto",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._aws_credentials_id = aws_credentials_id
        self._redshift_conn_id = redshift_conn_id

        self._table = table
        self._s3_path = s3_path
        self._json_path = json_path

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
            credentials.secret_key,
            self._json_path
        )
        redshift_hook.run(sql_stmt)

        self.log.info(f'Data copied from {self._s3_path} to {self._table}')
