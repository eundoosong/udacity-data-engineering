from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults
from .common_operator import CommonOperator


class StageToRedshiftOperator(CommonOperator):
    """
    Load data from s3 to target table
    """
    ui_color = '#358140'
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            json as '{}'
            """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id,
                 redshift_conn_id,
                 s3_bucket,
                 s3_key,
                 target_table,
                 json_option="auto",
                 *args, **kwargs):
        """
        :param aws_credentials_id: aws credentials id set by
                admin connections
        :param redshift_conn_id: redshift connection id
                set by admin connections
        :param s3_bucket: s3 bucket to load from
        :param s3_key: s3 object key to load from
        :param target_table: target table to copy to
        :param json_option: json option in redshift copy (default: auto)
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_path = f"s3://{s3_bucket}/{s3_key}"
        self.target_table = target_table
        self.json_option = json_option

    def _execute(self, context):
        keepalive_kwargs = {
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 5,
            "keepalives_count": 5,
        }
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id,
                                **keepalive_kwargs)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.target_table}")

        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_option,
        )
        redshift.run(formatted_sql)
