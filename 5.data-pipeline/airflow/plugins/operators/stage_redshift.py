from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator


class StageToRedshiftOperator(BaseOperator):
    """
    Load data from s3 to target table
    """
    template_fields = ('s3_key',)
    ui_color = '#358140'
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            {}
            """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id,
                 redshift_conn_id,
                 s3_bucket,
                 s3_key,
                 target_table,
                 file_format="json",
                 json_path="auto",
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
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.target_table = target_table
        self.file_format = file_format
        self.json_path = json_path

    def execute(self, context):
        if not (self.file_format == 'csv' or self.file_format == 'json'):
            raise ValueError(f'file format {self.file_format} is not csv or json')
        file_format_option = f"format json '{self.json_path}'" if self.file_format == 'json' \
            else 'format CSV'
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from destination Redshift table')
        redshift.run(f'DELETE FROM {self.target_table}')

        self.log.info('Copying data from S3 to Redshift')
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            file_format_option,
        )
        redshift.run(formatted_sql)
        self.log.info(f'{self.target_table} loaded..')
