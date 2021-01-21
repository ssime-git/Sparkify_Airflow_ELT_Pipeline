# Libraries:
# ---------
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class StageToRedshiftOperator(BaseOperator):
    """
    This function copy data from JSON or CSV files in S3
    And load the data into a redshift Data warehouse.
    
    Inputs: redshift_conn_id, table name, bucket name & key path, delimiter (if CSV), json option (auto or JsonPath)
    Output: table loading
    """
    
    # Color definition:
    # ----------------
    ui_color = '#358140'
    
    # Template field for the key path:
    # ---------------------------
    template_fields = ("s3_key",)
    
    # SQL statements for the copy operation:
    # -------------------------------------
    # copy from CSV files for future use:
    copy_sql_cvs = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """
    # copy from JSON files:
    copy_sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Parameters list:
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1, # OPTION for CSV import: True / False
                 json="",          # OPTION for JSON: "auto" / JsonPath
                 drop_table_before_load = False, # OPTION for the first run: Trur / False
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.json = json

    def execute(self, context):
        # Amazon web service hook:
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # redshift hook:
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Loading table:
        # -------------
        logging.info("Copying data from S3 to Redshift")
        # S3 path definition:
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        # If data file format is CSV:
        if (self.json==""):
            logging.info("loading from CSV file")
            formatted_sql = StageToRedshiftOperator.copy_sql_cvs.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
                )
        elif (self.json!=""):
            # If data file format is JSON
            logging.info("loading from JSON file")
            formatted_sql = StageToRedshiftOperator.copy_sql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json
                )
        #  
        redshift.run(formatted_sql)
        logging.info("Done Loading")