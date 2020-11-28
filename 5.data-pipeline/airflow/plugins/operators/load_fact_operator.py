from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator


class LoadFactOperator(BaseOperator):
    """
    Load fact data into target table
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 sql_load_query,
                 redshift_conn_id,
                 *args, **kwargs):
        """
        :param sql_load_query: sql load query tuple
                that consists of 'target table' and 'sql query'
        :param redshift_conn_id: redshift connection id
                set by admin connections
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = sql_load_query[0]
        self.load_sql = sql_load_query[1]

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.load_sql)
        self.log.info(f"fact table({self.target_table}) loaded..")
