from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.decorators import apply_defaults
from psycopg2.errors import UniqueViolation

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                postgres_conn_id="postgres",
                redshift_conn_id="redshift",
                query="",
                target_table="",
                truncate=True,
                use_redshift=True,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.redshift_conn_id=redshift_conn_id
        self.query=query
        self.target_table=target_table
        self.truncate=truncate
        self.use_redshift=use_redshift

    def execute(self, context):

        if self.use_redshift:

            redshift_hook = PostgresHook(self.redshift_conn_id)

            if self.truncate:
                redshift_hook.run(f"""truncate {self.target_table};""")
                redshift_hook.run(f"""insert into {self.target_table} ({self.query})""")

            else:
                redshift_hook.run(f"""insert into {self.target_table} ({self.query})""")

        if not self.use_redshift:

            postgres_hook=PostgresHook(self.postgres_conn_id)

            if self.truncate:
                with postgres_hook.get_conn() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(f"""truncate {self.target_table};""")
                        cursor.execute(f"""insert into {self.target_table} ({self.query})""")

            else:
                with postgres_hook.get_conn() as conn:
                    with conn.cursor() as cursor:
                        try:
                            cursor.execute(f"""insert into {self.target_table} ({self.query})""")
                        except UniqueViolation as e:
                            self.log.info(e)