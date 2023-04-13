from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2.errors import UniqueViolation

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                postgres_conn_id="postgres",
                redshift_conn_id="redshift",
                target_table="",
                query="",
                use_redshift=True,
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.redshift_conn_id=redshift_conn_id
        self.target_table=target_table
        self.query=query
        self.use_redshift=use_redshift

    def execute(self, context):

        if self.use_redshift:

            redshift_hook = PostgresHook(self.redshift_conn_id)
            redshift_hook.run(f"""insert into {self.target_table} ({self.query})""")

        if not self.use_redshift:

            postgres_hook=PostgresHook(self.postgres_conn_id)
            with postgres_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    # For fact table, insert
                    try:
                        cursor.execute(f"""insert into {self.target_table} ({self.query})""")
                    except UniqueViolation as e:
                        self.log.info(e)
