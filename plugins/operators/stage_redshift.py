from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 aws_region,
                 s3_bucket,
                 s3_key,
                 table,
                 json_path,
                 *args, **kwargs):

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.aws_region = aws_region
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.json_path = json_path

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}/'
        self.log.info('Start Redshift connection.')
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Deleteting old data from Redshift Table.')
        delete_statement = f"""DELETE FROM {self.table};"""
        redshift_conn.run(delete_statement)

        self.log.info('Copying data from S3 to Redshift.')
        copy_statement = f"""
            COPY {self.table} 
            FROM '{s3_path}'
            ACCESS_KEY_ID '{aws_credentials.access_key}' SECRET_ACCESS_KEY '{aws_credentials.secret_key}' 
            REGION '{self.aws_region}'
            JSON '{self.json_path}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
        """
        redshift_conn.run(copy_statement)
