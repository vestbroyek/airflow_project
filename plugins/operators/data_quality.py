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
                use_redshift=True,
                checks=[{"name": "has_rows", "sql": "select count(*)", "fail_result": 0}],
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables 
        self.use_redshift=use_redshift
        self.checks=checks

    def execute(self, context):

        if self.use_redshift:

            redshift_hook = PostgresHook(self.redshift_conn_id)
            self.log.info(f"Performing data quality checks...")

            for check in self.checks:
                self.log.info(f"Running check {check['name']}...")

                for table in self.tables:
                    result=redshift_hook.get_records(f"""{check['sql']} from {table}""")
                    if result==check['fail_result']:
                        self.log.info(f"Data quality check for {table} failed!")
                        raise ValueError 
                    else:
                        self.log.info(f"Data quality check for {table} passed!")

        if not self.use_redshift:

            postgres_hook=PostgresHook(self.postgres_conn_id)
            self.log.info(f"Performing data quality checks...")

            for table in self.tables:
                self.log.info(f"Running check {check['name']}...")

                for table in self.tables:
                    result=postgres_hook.get_records(f"""select count(*) from {table}""")
                    if result==check['fail_result']:
                        self.log.info(f"Data quality check for {table} failed!")
                        raise ValueError 
                    else:
                        self.log.info(f"Data quality check for {table} passed!")