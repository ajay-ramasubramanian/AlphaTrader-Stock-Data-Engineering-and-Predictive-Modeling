from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

# Load environment variables from .env file
from transformations.utils import MinioRetriever
from io import StringIO
import os
from dotenv import load_dotenv

load_dotenv()

class LoadFactOperator(BaseOperator):
    """
    Custom Airflow operator to load fact data into a Postgres table
    from a Minio bucket. This operator truncates the table before 
    inserting new data.
    """

    # Constructor for the operator
    def __init__(self,
                topic,  # Topic name used to retrieve data from Minio
                table_name,  # Target Postgres table name
                postgres_conn_id='postgres-warehouse',  # Postgres connection ID defined in Airflow
                minio_conn_id='minio',  # Minio connection ID defined in Airflow
                *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.topic = topic
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id
        self.minio_conn_id = minio_conn_id

    def execute(self, context):
        """
        The main logic of the operator. This method is executed when the task runs.
        It retrieves data from Minio, formats it as CSV, truncates the target table,
        and loads the data into Postgres.
        """
        
        self.log.info(f"Starting LoadFactOperator for table {self.table_name}")
        
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
                    # Truncate the table to clear any existing data
                    cur.execute(f"TRUNCATE TABLE {self.table_name}")
                    self.log.info(f"Copying data to fact table {self.table_name}")
                    cur.copy_from(output, self.table_name, null="")  # Copy CSV data to Postgres table
                    
                conn.commit()  # Commit the transaction

            self.log.info(f"Successfully loaded data into fact table {self.table_name}")

        except Exception as e:
            # Log any errors that occur during the execution
            self.log.error(f"An error occurred while loading fact data: {str(e)}")
            raise  # Raise the error to fail the task if necessary
