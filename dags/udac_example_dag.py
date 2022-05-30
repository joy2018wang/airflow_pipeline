from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators import StageToRedshiftOperato
# from operators. import LoadFactOperar
# # from airflow.operators import LoadDimensionOpetor
# from airflow.operators import DataQualityOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
# 'log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json',
#datetime(2018, 11, 1)
# The DAG does not have dependencies on past runs
# On failure, the task are retried 3 times
# Retries happen every 5 minutes
# Catchup is turned off
# Do not email on retry
default_args = {
    'owner': 'joy',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False
}

dag = DAG('udac_example_dag1',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          # schedule_interval = '0 2 * * *'; 2 am 
          schedule_interval='@monthly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11/2018-11-13-events.json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A/",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table='songplays',
    load_sql=SqlQueries.songplay_table_insert,
    dag=dag
)

#LoadDimensionOperator
load_user_dimension_table = LoadFactOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table='users',
    truncate = True,
    load_sql=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadFactOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table='songs',
    truncate = True,
    load_sql=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadFactOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table='artists',
    truncate = True,
    load_sql=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadFactOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table = 'time',
    truncate = True,
    load_sql = SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tablelist = ['songplays', 'time', 'songs',
                    'artists', 'users'],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# start_operator >> create_table
# create_table >> stage_events_to_redshift
# create_table >> stage_songs_to_redshift
start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift


stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

