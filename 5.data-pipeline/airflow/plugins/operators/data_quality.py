from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Check data quality
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 target_table_columns,
                 *args, **kwargs):
        """
        :param redshift_conn_id: redshift connection id set by admin connections
        :param target_table_columns: dict type for tables to be checked for data quality,
                                     that contains a table as key and columns as value in list
                                     for empty and null check.
                                     for example,
                                     target_table_columns = { "my_table": ["column1", "column2"] }
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table_columns = target_table_columns

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table, cols in self.target_table_columns.items():
            records = redshift.get_records(f"select count(*) from {table}")
            if len(records) == 0:
                raise ValueError(f"{table} is empty")
            for col in cols:
                records = redshift.get_records(
                    f"select count(*) from {table} where {col} = '' or {col} is null")
                if len(records) > 0:
                    raise ValueError(f"empty(or null) {col} found in {table} table")
        self.log.info("all data quality check passed")
