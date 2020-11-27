from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator


class LoadOperator(BaseOperator):
    """
    Load data into target table
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 sql_load_query,
                 redshift_conn_id,
                 truncate_insert=False,
                 *args, **kwargs):
        """
        :param sql_load_query: sql load query tuple
                that consists of 'target table' and 'sql query'
        :param redshift_conn_id: redshift connection id
                set by admin connections
        :param truncate_insert: flag if table is deleted before load
                (default: False)
        """
        super(LoadOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = sql_load_query[0]
        self.load_sql = sql_load_query[1]
        self.truncate_insert = truncate_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_insert is True:
            self.log.info(f"Clearing data from destination {self.target_table} table")
            redshift.run(f"DELETE FROM {self.target_table}")
        redshift.run(self.load_sql)
