from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                postgres_conn_id="postgres",
                redshift_conn_id="redshift",
                tables=[],
                type='has_rows',
                use_redshift=True,
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables 
        self.type=type
        self.use_redshift=use_redshift

    def execute(self, context):

        if self.use_redshift:

            redshift_hook = PostgresHook(self.redshift_conn_id)

            if self.type=="has_rows":
                self.log.info(f"Checking whether {self.tables} have rows...")
                for table in self.tables:
                    result=redshift_hook.get_records(f"""select count(*) from {table}""")
                    if result==0:
                        self.log.info(f"Data quality check for {table} failed!")
                        raise ValueError 
                    else:
                        self.log.info(f"Data quality check for {table} passed!")

        if not self.use_redshift:

            postgres_hook=PostgresHook(self.postgres_conn_id)

            if self.type=="has_rows":
                self.log.info(f"Checking whether {self.tables} have rows...")
                for table in self.tables:
                    result=postgres_hook.get_records(f"""select count(*) from {table}""")
                    if result==0:
                        self.log.info(f"Data quality check for {table} failed!")
                        raise ValueError 
                    else:
                        self.log.info(f"Data quality check for {table} passed!")
