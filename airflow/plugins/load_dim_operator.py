from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

# Load environment variables (e.g., credentials) from .env file
from airflow.utils.decorators import apply_defaults  # Remove if using Airflow 2.0+
from transformations.utils import MinioRetriever
from io import StringIO
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class LoadDimOperator(BaseOperator):
    """
    Custom Airflow operator to load dimensional data into a Postgres table
    from a Minio bucket. This operator can either append data or truncate
    the table before inserting new data, based on the provided parameters.
    """

    # Constructor for the operator
    def __init__(self, 
                topic,  # Topic name used to retrieve data from Minio
                table_name,  # Target Postgres table name
                postgres_conn_id='postgres-warehouse',  # Postgres connection ID defined in Airflow
                minio_conn_id='minio',  # Minio connection ID defined in Airflow
                append=True,  # If True, append data; if False, truncate before inserting
                *args, **kwargs):
        super(LoadDimOperator, self).__init__(*args, **kwargs)
        self.topic = topic
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id
        self.minio_conn_id = minio_conn_id
        self.append = append

    def execute(self, context):
        """
        The main logic of the operator. This method is executed when the task runs.
        It retrieves data from Minio, formats it as CSV, and loads it into a Postgres table.
        """
        
        self.log.info(f"Starting LoadDimOperator for table {self.table_name}")
        
        try:
            # Retrieve data from Minio for the given topic
            minio_retriever = MinioRetriever(os.getenv('USER_NAME'), self.topic, 'presentation', self.minio_conn_id)
            df = minio_retriever.retrieve_object()  # Fetch data as a DataFrame
            
            # Convert the DataFrame to CSV format for loading into Postgres
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)  # Move cursor to the start of the StringIO object

            # Establish a connection to Postgres
            pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Optionally truncate the table if append is False
                    if not self.append:
                        self.log.info(f"Truncating table {self.table_name}")
                        cur.execute(f"TRUNCATE TABLE {self.table_name}")
                    
                    # Load the data into the table
                    self.log.info(f"Copying data to table {self.table_name}")
                    cur.copy_from(output, self.table_name, null="")  # Copy CSV data to Postgres table
                    
                conn.commit()  # Commit the transaction

            self.log.info(f"Successfully loaded data into {self.table_name}")

        except Exception as e:
            # Log any errors that occur during the execution
            self.log.error(f"An error occurred while loading data: {str(e)}")
            raise  # Raise the error to fail the task if necessary
