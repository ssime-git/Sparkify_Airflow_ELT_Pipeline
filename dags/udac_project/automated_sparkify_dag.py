# Main DAG pour the automated ETL:
# ------------------------------

# Default airflow libraries:
#---------------------------
from datetime import datetime, timedelta
#import os # not needed
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
#
#from airflow.operators.subdag_operator import SubDagOperator # if subdag needed
#from udac_project.create_table_load_subdag import drop_create_tables_subdag # if subdag needed

# Import custom plugins libraries:
# --------------------------------
# Custom Operators:
from udac_plugins.udac_operators import (StageToRedshiftOperator, LoadFactOperator,
                                         LoadDimensionOperator, DataQualityOperator)
# SQL Queries
from udac_plugins.udac_helpers import SqlQueries
#from udac_plugins.udac_helpers import SqlQueries.create_songplays_table


# Credentials: (if needed)
#------------------------
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# DAG default parameters definitions:
#------------------------------------
default_args = {
    # DAG owner name
    'owner': 'sparkify',
    # Define a start & end date
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 2),
    # Don't depends on past run
    'depends_on_past': False,
    #On failure, the task are retried 3 times
    'retries': 3,
    #Retries happen every 5 minutes
    'retry_delay': timedelta(minutes=5),
    #Do not email on retry
    'email_on_retry': False,
    #When turned off, the scheduler creates a DAG run only for the latest interval
    'catchup': False
}

# DAGs definition:
#-----------------
dag = DAG(
          # Name of the DAG:
          'sparkify_automated_dag',
          # DAG default parameters:
          default_args=default_args,
          # Description on the DAG Work
          description='Load and transform data in Redshift with Airflow',
          # run @hourly equivalent to  '0 * * * *'
          schedule_interval='0 * * * *',
          # for only one run at a time
          max_active_runs=1
        )

# Tasks definition:
# -----------------
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# Load staging events task:
load_staging_events_table_task = StageToRedshiftOperator(
    task_id = "load_staging_events_from_s3_to_redshift",
    dag = dag,
    table = "staging_events",
    redshift_conn_id = "redshift",
    aws_credentials_id= "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_data/{execution_date.year}/{execution_date.month}",
    json = "s3://udacity-dend/log_json_path.json"
)

# Load staging songs task:
load_staging_songs_table_task = StageToRedshiftOperator(
    task_id = "load_staging_songs_from_s3_to_redshift",
    dag = dag,
    table = "staging_songs",
    redshift_conn_id = "redshift",
    aws_credentials_id= "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    json = "auto"
)

# Loading Fact table songplays:
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songplays",
    sql_statement = SqlQueries.songplay_table_insert,
)

# Loading dimensions table songplays:
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    sql_statement = SqlQueries.user_table_insert,
    table = "users",
    loading_mode = "truncate-insert"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    sql_statement = SqlQueries.song_table_insert,
    table = "songs",
    loading_mode = "truncate-insert"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    sql_statement = SqlQueries.artist_table_insert,
    table = "artists",
    loading_mode = "truncate-insert"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    sql_statement = SqlQueries.time_table_insert,
    table = "time",
    loading_mode = "truncate-insert"
)

# Run quality check:
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    params = {
        # Check type (in Sql Queries) : {table: "table_name", columns: "columns_list}
        "standard_isnull_check" : {"table": 'songplays', "columns": ['playid', 'start_time', 'userid']},
        "standard_isnull_check" : {"table": 'artists', "columns": ['artistid']}
    }
)

# Tasks end:
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Tasks ordering:
# --------------
start_operator >> [load_staging_events_table_task, load_staging_songs_table_task]
#
[load_staging_events_table_task, load_staging_songs_table_task] >> load_songplays_table
#
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
#
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
#
run_quality_checks >> end_operator