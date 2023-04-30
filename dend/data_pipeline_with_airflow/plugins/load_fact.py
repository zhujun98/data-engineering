from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query_statement="",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._redshift_conn_id = redshift_conn_id
        self._sql_query_statement = sql_query_statement

    def execute(self, context):
        redshift_hook = PostgresHook(self._redshift_conn_id)
        self.log.info(f'Loading data into fact table')
        redshift_hook.run(self._sql_query_statement)
        self.log.info(f'Data loaded into fact table')
