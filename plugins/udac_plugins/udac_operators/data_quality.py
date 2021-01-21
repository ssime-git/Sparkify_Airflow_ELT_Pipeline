from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

# SQL Queries
from udac_plugins.udac_helpers import SqlQueries

class DataQualityOperator(BaseOperator):
    """
    This operator performs data quality check operations with table and table_column 
    OR ONLY with params.
    
    List of arguments:
    - redshift_conn_id
    
    Either you provide (for the standard check):
    - table : the table on which the check is performed
    - table_column: the table column on which the check is performed
    
    OR ONLY:
    - params: list of checks to perform
    
    The standard check is the not null check for any column provided.
    Other check can be implemented in SqlQueries and here.
    """
    # Box color:
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Parameter list:
                 redshift_conn_id="",
                 table="",
                 table_column="",
                 params={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Parameter Map:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_column = table_column,
        self.params = params

    def execute(self, context):
        # redshift hook:
        redshift_hook = PostgresHook(self.redshift_conn_id)
        #
        if (len(self.params)== 0 and self.table !="" and self.table_column!=""):
            # Standard is null check:
            logging.info("single column check!")
            records = reshift_hook.get_records(SqlQueries.standard_isnull_check.format(self.table, self.table_column))
            #
            if len(records) < 1 or len(records[0]) < 1:
                         raise ValueError(f"Data quality check failed. {self.table} returned no results")
            num_records = records[0][0]
            #
            if num_records < 1:
                         raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
            logging.info("single data quality check performed !")
                         
        else:
            # more sophisticated checks:
            for to_check, items in self.params.items():
                if (to_check == "standard_isnull_check"):
                    
                    # get the table name:
                    tab_name = items['table']
                    
                    # iterate on columns list:
                    for col in items['columns']:
                        #
                        records = redshift_hook.get_records(SqlQueries.standard_isnull_check.format(tab_name, col))
                        #
                        if len(records) < 1 or len(records[0]) < 1:
                            raise ValueError(f"Data quality check failed. {self.table} returned no results")
                            #
                        logging.info(f"Data quality check pass for {col} ")
                        #
                    num_records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {tab_name}")
                    logging.info(f"{tab_name} has {num_records} records")
                        #
                 # to complete whit other checks if necessary