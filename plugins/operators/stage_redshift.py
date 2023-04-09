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
                 conn_id="postgres",
                 bucket="maurits-westbroek",
                 key='song-data/TRAAAAK128F9318786.json',
                 target_table="staging_songs",
                 target_schema="public",
                 date_field=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.bucket=bucket
        self.key=key
        self.target_table=target_table
        self.target_schema=target_schema
        self.date_field=date_field

    def execute(self, context):
        # Download files from S3
        s3hook=S3Hook("aws")

        # If date field=True, make the key dynamic to logical execution date
        if self.date_field:
            self.log.info(f"Attempting to stage {context['ds']+self.key}")
            obj=s3hook.get_conn().get_object(
                Bucket=self.bucket,
                Key=context['ds']+self.key
            )

        else:
            self.log.info(f"Attempting to stage {self.key}")
            obj=s3hook.get_conn().get_object(
                Bucket=self.bucket,
                Key=self.key
            )

        # Read JSON 
        self.log.info(obj)
        
        # turn into df
        df=pd.read_json(obj['Body'], lines=True, orient='records')

        # Copy into DB
        postgres_hook=PostgresHook(self.conn_id)
        engine=postgres_hook.get_sqlalchemy_engine()
        df.to_sql(self.target_table, engine, if_exists='replace', index=False)
        engine.dispose()