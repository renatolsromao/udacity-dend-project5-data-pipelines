from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 14),
    'email': ['contato@renato.dev'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('sparkify_analytics_dag',
          default_args=default_args,
          description='Load and transform Sparkify data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift_sparkify',
    aws_credentials_id='aws_credentials',
    aws_region='us-east-1',
    s3_bucket='rlsr-dend/sparkify',
    s3_key='log-data',
    table='staging_events',
    json_path='s3://rlsr-dend/sparkify/log_json_path.json',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift_sparkify',
    aws_credentials_id='aws_credentials',
    aws_region='us-east-1',
    s3_bucket='rlsr-dend/sparkify',
    s3_key='song_data',
    table='staging_songs',
    json_path='auto',
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift_sparkify',
    destination_table='songplays',
    insert_select_statement=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift_sparkify',
    destination_table='users',
    insert_select_statement=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift_sparkify',
    destination_table='songs',
    insert_select_statement=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift_sparkify',
    destination_table='artists',
    insert_select_statement=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift_sparkify',
    destination_table='time',
    insert_select_statement=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift_sparkify',
    rules=[
        {'query': SqlQueries.artist_table_size, 'op': lambda x: x > 0},
        {'query': SqlQueries.time_table_size, 'op': lambda x: x > 0},
        {'query': SqlQueries.user_table_size, 'op': lambda x: x > 0},
        {'query': SqlQueries.song_table_size, 'op': lambda x: x > 0},
        {'query': SqlQueries.songplay_table_size, 'op': lambda x: x > 0},
        {'query': SqlQueries.artist_table_fields_null, 'op': lambda x: x == 0},
        {'query': SqlQueries.time_table_fields_null, 'op': lambda x: x == 0},
    ]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
