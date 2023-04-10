from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2.errors import UniqueViolation

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                postgres_conn_id="postgres",
                target_table="",
                query="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.target_table=target_table
        self.query=query

    def execute(self, context):
        postgres_hook=PostgresHook(self.postgres_conn_id)
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # For fact table, insert
                try:
                    cursor.execute(f"""insert into {self.target_table} ({self.query})""")
                except UniqueViolation as e:
                    self.log.info(e)
