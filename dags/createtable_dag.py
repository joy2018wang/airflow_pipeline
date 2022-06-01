from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from helpers.sql_create_table_queries import create_tables_sql

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
    'start_date': datetime.now(),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False
}

dag = DAG('createtable_dag',
          default_args=default_args,
          description='create table for the first time run',
          schedule_interval=None
        )


create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql
)



