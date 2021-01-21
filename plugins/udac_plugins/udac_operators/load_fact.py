from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadFactOperator(BaseOperator):
    """
    This function allow to load data from staging table to songplays table
    inputs:
    - redshift_conn_id the redshift connection ID
    - table: the name of the table
    - sql: the select sql statement
    """
    
    # insert SQL statements:
    # ----------------------
    insert_sql = """
        BEGIN;
        INSERT INTO {} ({});
        COMMIT;
    """
    # NOT USED
    append_sql ="""
        ALTER TABLE {} APPEND FROM ({});
    """
    
    # Color box definition:
    # ---------------------
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # List of parameters
                 redshift_conn_id="",
                 table="",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_statement=sql_statement

    def execute(self, context):
        logging.info(f"loading fact {self.table} table")
        #
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #
        formatted_sql = LoadFactOperator.insert_sql.format(self.table, self.sql_statement)
        #
        redshift_hook.run(formatted_sql)
        #
        logging.info(f"{self.table} loaded!!")
        
        
