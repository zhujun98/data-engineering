import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HasRowsOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *, redshift_conn_id, table, **kwargs):
        super().__init__(**kwargs)
        self._redshift_conn_id = redshift_conn_id
        self._table = table

    def execute(self, context):
        """Override."""
        redshift_hook = PostgresHook(self._redshift_conn_id)
        records = redshift_hook.get_records(
            f"SELECT COUNT(*) FROM {self._table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. "
                             f"{self._table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. "
                             f"{self._table} contained 0 rows")
        logging.info(f"Data quality on table {self._table} "
                     f"check passed with {records[0][0]} records")
