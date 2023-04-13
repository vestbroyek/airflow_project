from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import io
import json 
import pandas as pd 

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                postgres_conn_id="postgres",
                redshift_conn_id="redshift",
                aws_conn_id="aws",
                bucket="maurits-westbroek-airflow",
                folder="",
                filename="",
                target_table="",
                target_schema="public",
                date_field=False,
                if_exists='replace',
                use_redshift=True,
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.redshift_conn_id=redshift_conn_id
        self.aws_conn_id=aws_conn_id
        self.bucket=bucket
        self.folder=folder 
        self.filename=filename
        self.target_table=target_table
        self.target_schema=target_schema
        self.date_field=date_field
        self.if_exists=if_exists
        self.use_redshift=use_redshift

    def execute(self, context):

        if self.use_redshift and not self.date_field:

            redshift_hook = PostgresHook(self.redshift_conn_id)
            redshift_hook.run(f"""
                copy {self.target_table}
                from 's3://{self.bucket}/{self.folder}/{self.filename}' 
                iam_role 'arn:aws:iam::173217485177:role/my-redshift-service-role'
                format as json 'auto';
            """)

        # allow use of date field
        if self.use_redshift and self.date_field:

            redshift_hook = PostgresHook(self.redshift_conn_id)
            redshift_hook.run(f"""
                copy {self.target_table}
                from 's3://{self.bucket}/{self.folder}/{context['ds']}+{self.filename}' 
                iam_role 'arn:aws:iam::173217485177:role/my-redshift-service-role'
                format as json 'auto';
            """) 

        if not self.use_redshift:

            # Download files from S3
            s3hook=S3Hook(self.aws_conn_id)

            # If date_field=True, make the key dynamic to logical execution date
            if self.date_field:
                self.log.info(f"Attempting to stage {self.folder+context['ds']+self.filename}")
                obj=s3hook.get_conn().get_object(
                    Bucket=self.bucket,
                    Key=self.folder+context['ds']+self.filename
                )

            else:
                self.log.info(f"Attempting to stage {self.folder+self.filename}")
                obj=s3hook.get_conn().get_object(
                    Bucket=self.bucket,
                    Key=self.folder+self.filename
                )
            
            # turn into df
            df=pd.read_json(obj['Body'], lines=True, orient='records')

            # Copy into DB
            postgres_hook=PostgresHook(self.postgres_conn_id)
            engine=postgres_hook.get_sqlalchemy_engine()
            df.to_sql(self.target_table, engine, if_exists=self.if_exists, index=False)
            engine.dispose()