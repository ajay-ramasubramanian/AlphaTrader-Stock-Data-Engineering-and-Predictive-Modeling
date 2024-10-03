from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from transformations.utils import MinioRetriever
from io import StringIO
import os
from dotenv import load_dotenv
load_dotenv()
class LoadFactOperator(BaseOperator):

    def __init__(self,
                topic,
                table_name,
                postgres_conn_id='postgres-warehouse',
                minio_conn_id='minio',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.topic = topic
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id
        self.minio_conn_id = minio_conn_id

    def execute(self, context):
        self.log.info(f"Starting LoadFactOperator for table {self.table_name}")
        
        try:
            # Retrieve data from Minio
            minio_retriever = MinioRetriever(os.getenv('USER_NAME'), self.topic, 'presentation', self.minio_conn_id)
            df = minio_retriever.retrieve_object()
            
            # Prepare data for Postgres
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)

            # Connect to Postgres and load data
            pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {self.table_name}")
                    self.log.info(f"Copying data to fact table {self.table_name}")
                    cur.copy_from(output, self.table_name, null="")
                    
                conn.commit()

            self.log.info(f"Successfully loaded data into fact table {self.table_name}")

        except Exception as e:
            self.log.error(f"An error occurred while loading fact data: {str(e)}")
            raise