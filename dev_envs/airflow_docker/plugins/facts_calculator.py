from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FactsCalculatorOperator(BaseOperator):
    _FACTS_CALCULATOR_SQL = """
        DROP TABLE IF EXISTS {destination_table};
        CREATE TABLE {destination_table} AS
        SELECT
            {groupby_column},
            MAX({fact_column}) AS max_{fact_column},
            MIN({fact_column}) AS min_{fact_column},
            AVG({fact_column}) AS average_{fact_column}
        FROM {origin_table}
        GROUP BY {groupby_column};
    """
    @apply_defaults
    def __init__(self, *,
                 redshift_conn_id: str,
                 origin_table: str,
                 destination_table: str,
                 fact_column: str,
                 groupby_column: str,
                 **kwargs):
        super().__init__(**kwargs)
        self._redshift_conn_id = redshift_conn_id
        self._origin_table = origin_table
        self._destination_table = destination_table
        self._fact_column = fact_column
        self._groupby_column = groupby_column

    def execute(self, context):
        """Override."""
        redshift = PostgresHook(postgres_conn_id=self._redshift_conn_id)

        sql_stmt = self._FACTS_CALCULATOR_SQL.format(
            origin_table=self._origin_table,
            destination_table=self._destination_table,
            fact_column=self._fact_column,
            groupby_column=self._groupby_column
        )
        redshift.run(sql_stmt)
