from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadDimensionOperator(BaseOperator):
    """
    This function allow to create and load data from staging_tables to
    fact and dimension tables.
    inputs:
        - redshift_conn_id: redshift connection ID
        - table: name of the table to load
        - Loading mode between "append" only and "truncate-insert" modes
    """
    # color box:
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Parameters list:
                 redshift_conn_id="",
                 sql_statement="",
                 table="",
                 loading_mode="",   #"append" or "truncate-insert"
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Parameters map
        self.redshift_conn_id=redshift_conn_id
        self.sql_statement=sql_statement
        self.table=table
        self.loading_mode=loading_mode

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if (self.loading_mode=="append"):
            # loading_mode = "append" only
            logging.info(f"Appending_in_{self.table}")
            redshift_hook.run(f"ALTER TABLE {self.table} APPEND FROM {self.sql_statement}")
            #
        elif (self.loading_mode=="truncate-insert"):
            #truncate-insert pattern
            logging.info(f"Cleaning {self.table} table")
            
            #1: clearing
            redshift_hook.run(f"TRUNCATE {self.table}")
            
            #2: inserting
            logging.info(f"Inserting {self.table} table")
            redshift_hook.run(f"INSERT INTO {self.table} {self.sql_statement}")
        #
        logging.info(f'completed to load {self.table} table')
        
