from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                postgres_conn_id="postgres",
                tables=[],
                type='has_rows',
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.tables=tables 
        self.type=type

    def execute(self, context):
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
