from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import pandas as pd
import pendulum 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2018,11,1)
}

dag = DAG('prod_dag',
          default_args=default_args,
          catchup=False,
          description='Load and transform data in Postgres with Airflow',
          max_active_runs=1,
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    folder='log-data',
    filename='2018-11-11-events.json',
    date_field=False,
    target_table='staging_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    folder='song-data',
    filename='TRAAAAK128F9318786.json',
    target_table='staging_songs',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    target_table="songplays",
    query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    target_table="users",
    query=SqlQueries.user_table_insert,
    truncate=True,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    target_table="songs",
    query=SqlQueries.song_table_insert,
    truncate=True,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    target_table="artists",
    query=SqlQueries.artist_table_insert,
    truncate=True,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    target_table="time",
    query=SqlQueries.time_table_insert,
    truncate=True,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    tables=["songplays"],
    dag=dag
)

end_operator = DummyOperator(task_id='stop_execution', dag=dag)

# dependencies
start_operator >> (stage_events_to_redshift, stage_songs_to_redshift) >> load_songplays_table \
 >> (load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table) \
 >> run_quality_checks >> end_operator