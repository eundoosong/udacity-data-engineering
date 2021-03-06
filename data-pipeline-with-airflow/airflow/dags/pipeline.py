from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators import (StageToRedshiftOperator,
                               LoadDimensionOperator, LoadFactOperator, DataQualityOperator)
from plugins.helpers import SqlQueries

redshift_conn_id = 'redshift_conn_id'
aws_credentials_id = 'aws_credentials'
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 12),
    'catchup_by_default': False,
}

dag = DAG('udacity_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False,
          )

with dag:
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        aws_credentials_id=aws_credentials_id,
        redshift_conn_id=redshift_conn_id,
        s3_bucket='udacity-dend',
        s3_key='log_data',
        target_table='staging_events',
        file_format='json',
        json_path='s3://udacity-dend/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        aws_credentials_id=aws_credentials_id,
        redshift_conn_id=redshift_conn_id,
        s3_bucket='udacity-dend',
        s3_key='song_data',
        target_table='staging_songs',
        file_format='json',
        json_path='auto'
    )

    load_songplays_fact_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id=redshift_conn_id,
        sql_load_query=SqlQueries.songplays_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id=redshift_conn_id,
        sql_load_query=SqlQueries.users_table_insert,
        mode='truncate',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id=redshift_conn_id,
        sql_load_query=SqlQueries.songs_table_insert,
        mode='truncate',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id=redshift_conn_id,
        sql_load_query=SqlQueries.artists_table_insert,
        mode='truncate',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id=redshift_conn_id,
        sql_load_query=SqlQueries.time_table_insert,
        mode='truncate',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id=redshift_conn_id,
        target_table_columns={
            'songplays': ['userid'],
            'users': ['first_name', 'last_name', 'level'],
            'songs': ['title', 'artistid'],
            'artists': ['name'],
            'time': [],
        }
    )

    end_operator = DummyOperator(task_id='Stop_execution')

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_fact_table
load_songplays_fact_table >> [load_user_dimension_table, load_song_dimension_table,
                              load_artist_dimension_table, load_time_dimension_table] \
                            >> run_quality_checks >> end_operator
