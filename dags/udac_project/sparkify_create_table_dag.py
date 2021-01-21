# Default airflow libraries:
#---------------------------
from datetime import datetime, timedelta
#import os # not needed
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator # if subdag needed
from airflow.operators.postgres_operator import PostgresOperator
from udac_project.create_table_load_subdag import drop_create_tables_subdag # if subdag needed

# Import custom plugins libraries:
# --------------------------------
# Custom Operators:
from udac_plugins.udac_operators import (StageToRedshiftOperator, LoadFactOperator,
                                         LoadDimensionOperator, DataQualityOperator)
# SQL Queries
from udac_plugins.udac_helpers import SqlQueries

# DAG parameters definitions:
#----------------------------
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
          'sparkify_table_creation_dag',
          default_args=default_args,
          description="create all the necessary tables for sparkify DB",
          # run once
          schedule_interval='@once'
        )


# Tasks definition:
# -----------------

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Drop tables:
# -----------
redshift_conn_id = "redshift"
    
drop_tables_subtask = PostgresOperator(
   task_id="dropping_staging_events_table",
   dag = dag,
   postgres_conn_id=redshift_conn_id,
   sql=("""DROP TABLE IF EXISTS staging_events;
   DROP TABLE IF EXISTS staging_songs;
   DROP TABLE IF EXISTS songplays;
   DROP TABLE IF EXISTS artists;
   DROP TABLE IF EXISTS songs;
   DROP TABLE IF EXISTS time;
   DROP TABLE IF EXISTS users;
   """)
)
#
    
# Create tables:
#--------------
    
create_staging_events_table_task = PostgresOperator(
    task_id="create_staging_events_table",
    dag = dag,
    postgres_conn_id=redshift_conn_id,
    sql=SqlQueries.create_staging_events_table.format("staging_events")
)
    
create_staging_songs_table_task = PostgresOperator(
    task_id="create_staging_songs_table",
    dag = dag,
    postgres_conn_id=redshift_conn_id,
    sql=SqlQueries.create_staging_songs_table.format("staging_songs")
)
    
create_songplays_table_task = PostgresOperator(
    task_id="create_songplays_table",
    dag = dag,
    postgres_conn_id=redshift_conn_id,
    sql=SqlQueries.create_songplays_table
)

create_artists_table_task = PostgresOperator(
    task_id="create_artists_table",
    dag = dag,
    postgres_conn_id=redshift_conn_id,
    sql=SqlQueries.create_artists_table
)
    
create_songs_table_task = PostgresOperator(
    task_id="create_songs_table",
    dag = dag,
    postgres_conn_id=redshift_conn_id,
    sql=SqlQueries.create_songs_table
)

create_time_table_task = PostgresOperator(
    task_id="create_time_table",
    dag = dag,
    postgres_conn_id=redshift_conn_id,
    sql=SqlQueries.create_time_table
)

create_users_table_task = PostgresOperator(
    task_id="create_users_table",
    dag = dag,
    postgres_conn_id=redshift_conn_id,
    sql=SqlQueries.create_users_table
)

# Dummy transition: End creation
    # ------------------------------
end_creation = DummyOperator(task_id='Create_tables_operations_end',  dag=dag)

# Tasks ordering:
# ---------------
start_operator >> drop_tables_subtask
#
drop_tables_subtask >> [create_staging_events_table_task, create_staging_songs_table_task, 
                        create_songplays_table_task, create_artists_table_task, 
                        create_songs_table_task, create_time_table_task, create_users_table_task]
#
[create_staging_events_table_task, create_staging_songs_table_task, 
 create_songplays_table_task, create_artists_table_task, create_songs_table_task, 
 create_time_table_task, create_users_table_task] >> end_creation