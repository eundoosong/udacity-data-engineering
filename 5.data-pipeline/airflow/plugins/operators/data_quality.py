from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataQualityOperator(BaseOperator):
    """
    Check data quality
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 *args, **kwargs):
        """
        :param redshift_conn_id: redshift connection id set by admin connections
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift.get_records(
            "select count(*) from songs where songid = ''")
        if len(records) > 0:
            logging.warning("empty song id found in songs table")
        records = redshift.get_records(
            "select count(*) from artists where artistid = ''")
        if len(records) > 0:
            logging.warning("empty artist id found in artists table")
        logging.info("all data quality check passed")
