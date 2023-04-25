import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataCountCheckOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._redshift_conn_id = redshift_conn_id
        if tables is None:
            tables = []
        self._tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self._redshift_conn_id)

        for tb in self._tables:
            self.log.info(f"Checking data quality for table '{tb}'")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {tb}")
            if len(records[0]) < 1 or len(records) < 1:
                raise ValueError(f"Data quality check failed: "
                                 f"Query of table '{tb}' returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. "
                                 f"Table '{tb}' contained 0 rows")
            self.log.info(f"Data quality check on table '{tb}' "
                          f"passed with {records[0][0]} records")


class TripDateCheckOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._redshift_conn_id = redshift_conn_id
        self._table = "trip"

    def execute(self, context):
        redshift_hook = PostgresHook(self._redshift_conn_id)
        tb = self._table

        self.log.info(f"Checking data quality for table '{tb}'")
        records = redshift_hook.get_records(
            f"SELECT MIN(start_date), MAX(start_date) FROM {tb}")
        if len(records[0]) < 1 or len(records) < 1:
            raise ValueError(f"Data quality check failed: "
                             f"Query of table '{tb}' returned no results")
        assert records[0][0] == datetime.date(2018, 1, 1)
        assert records[0][1] == datetime.date(2021, 3, 31)
