from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators import (StageToRedshiftOperator,
                               LoadOperator, DataQualityOperator)
from plugins.helpers import SqlQueries

redshift_conn_id = "redshift_conn_id"
aws_credentials_id = "aws_credentials"
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id=aws_credentials_id,
    redshift_conn_id=redshift_conn_id,
    s3_bucket="udacity-dend",
    s3_key="log_data",
    target_table="staging_events",
    json_option="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id=aws_credentials_id,
    redshift_conn_id=redshift_conn_id,
    s3_bucket="udacity-dend",
    s3_key="song_data",
    target_table="staging_songs",
    json_option="auto"
)

load_songplays_fact_table = LoadOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    sql_load_query=SqlQueries.songplays_table_insert
)

load_user_dimension_table = LoadOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    sql_load_query=SqlQueries.users_table_insert,
    truncate_insert=True
)

load_song_dimension_table = LoadOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    sql_load_query=SqlQueries.songs_table_insert,
    truncate_insert=True
)

load_artist_dimension_table = LoadOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    sql_load_query=SqlQueries.artists_table_insert,
    truncate_insert=True
)

load_time_dimension_table = LoadOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
    sql_load_query=SqlQueries.time_table_insert,
    truncate_insert=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=redshift_conn_id,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_fact_table
stage_songs_to_redshift >> load_songplays_fact_table
load_songplays_fact_table >> load_user_dimension_table
load_songplays_fact_table >> load_song_dimension_table
load_songplays_fact_table >> load_artist_dimension_table
load_songplays_fact_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
