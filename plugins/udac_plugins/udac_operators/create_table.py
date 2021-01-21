from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class CreateTableOperator(BaseOperator):
    """
    This operator allow to create table based on an sql statement.
    Inputs: redshift_conn_id and a create_sql_statement
    """

    @apply_defaults
    def __init__(self,
                 # Input variables:
                 #-----------------
                 
                 redshift_conn_id="",
                 table="", # if needed for future use
                 create_sql_statement="",
                 drop_first = True, # if table exists
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table # if neede for future use
        self.create_sql_statement=create_sql_statement
        self.drop_first = drop_first

    def execute(self, context):
        redshift_hook = PostgresHook(redshift_conn_id)
        
        # drop first:
        if (drop_first):
            redshift_hook.run(f"DROP TABLE IF EXISTS {self.table}")
        
        # creating table:
        logging.info(f'Executing {create_sql_statement}')
        redshift_hook.run(create_sql_statement)
        self.log.info('Table created')