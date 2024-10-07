from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

# Load environment variables from .env file
from transformations.utils import MinioRetriever
from io import StringIO
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class LoadTransformationOperator(BaseOperator):
    """
    Custom Airflow operator to load transformation data into a Postgres table
    from a Minio bucket. This operator supports dynamic retrieval of objects
    by a key and can optionally truncate the target table before loading data.
    """
    # Define fields that can be templated (useful for dynamic task execution)
    template_fields = ('topic', 'table_name', 'key')

    def __init__(
        self,
        topic,  # Topic name used to retrieve data from Minio
        table_name,  # Target Postgres table name
        key,  # Specific key (file) to retrieve from Minio
        postgres_conn_id='postgres-warehouse',  # Postgres connection ID defined in Airflow
        minio_conn_id='minio',  # Minio connection ID defined in Airflow
        truncate=True,  # If True, truncate table before loading new data
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.table_name = table_name
        self.key = key
        self.postgres_conn_id = postgres_conn_id
        self.minio_conn_id = minio_conn_id
        self.truncate = truncate

    def execute(self, context):
        """
        The main logic of the operator. This method is executed when the task runs.
        It retrieves a specific object from Minio, formats it as CSV, and loads it 
        into a Postgres table, with an option to truncate the table first.
        """
        
        self.log.info(f"Starting LoadTransformationOperator for table {self.table_name}")
        
        try:
            # Retrieve a specific object (file) from Minio using the key
            minio_retriever = MinioRetriever(os.getenv('USER_NAME'), self.topic, 'presentation', self.minio_conn_id)
            df = minio_retriever.retrieve_object(key=self.key)  # Fetch data as a DataFrame
            
            # Convert the DataFrame to CSV format for loading into Postgres
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)  # Move cursor to the start of the StringIO object

            # Establish a connection to Postgres
            pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Optionally truncate the table if the truncate flag is set
                    if self.truncate:
                        self.log.info(f"Truncating table {self.table_name}")
                        cur.execute(f"TRUNCATE TABLE {self.table_name}")
                    
                    # Load the data into the table
                    self.log.info(f"Copying data to table {self.table_name}")
                    cur.copy_from(output, self.table_name, null="")  # Copy CSV data to Postgres table
                    
                conn.commit()  # Commit the transaction

            self.log.info(f"Successfully loaded data into table {self.table_name}")

        except Exception as e:
            # Log any errors that occur during the execution
            self.log.error(f"An error occurred while loading transformation data: {str(e)}")
            raise  # Raise the error to fail the task if necessary
