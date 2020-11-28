from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator


class LoadDimensionOperator(BaseOperator):
    """
    Load dimension data into target table
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 sql_load_query,
                 redshift_conn_id,
                 mode="truncate",
                 *args, **kwargs):
        """
        :param sql_load_query: sql load query tuple
                that consists of 'target table' and 'sql query'
        :param redshift_conn_id: redshift connection id
                set by admin connections
        :param mode: mode for truncate | insert (default: truncate)
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = sql_load_query[0]
        self.load_sql = sql_load_query[1]
        self.mode = mode

    def execute(self, context):
        mode = self.mode.lower()
        if mode != "truncate" and mode != "insert":
            raise ValueError("invalid mode (truncate and insert only support)")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if mode == "truncate":
            self.log.info(f"Clearing data from target {self.target_table} table")
            redshift.run(f"TRUNCATE {self.target_table}")
        redshift.run(self.load_sql)
        self.log.info(f"dimension table({self.target_table}) loaded..")
