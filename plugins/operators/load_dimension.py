from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 destination_table,
                 insert_select_statement,
                 *args, **kwargs):

        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.insert_select_statement = insert_select_statement

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        load_fact_statement = f"""
            DROP TABLE IF EXISTS {self.destination_table};
            CREATE TABLE {self.destination_table} AS
            {self.insert_select_statement}
        """
        redshift_conn.run(load_fact_statement)
