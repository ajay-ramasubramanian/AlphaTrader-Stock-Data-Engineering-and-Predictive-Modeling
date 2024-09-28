from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
# from airflow.utils.decorators import apply_defaults  # Remove if using Airflow 2.0+

from transformations.utils import MinioRetriever
from io import StringIO

class LoadTransformationOperator(BaseOperator):
    template_fields = ('topic', 'table_name', 'key')
    # @apply_defaults  # Remove if using Airflow 2.0+
    def __init__(
        self,
        topic,
        table_name,
        key,
        postgres_conn_id='postgres-warehouse',
        minio_conn_id='minio',
        truncate=True,
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
        self.log.info(f"Starting LoadTransformationOperator for table {self.table_name}")
        
        try:
            # Retrieve data from Minio
            minio_retriever = MinioRetriever('suhaas', self.topic, 'presentation', self.minio_conn_id)
            df = minio_retriever.retrieve_object(key=self.key)
            
            # Prepare data for Postgres
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)

            # Connect to Postgres and load data
            pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    if self.truncate:
                        self.log.info(f"Truncating table {self.table_name}")
                        cur.execute(f"TRUNCATE TABLE {self.table_name}")
                    
                    self.log.info(f"Copying data to table {self.table_name}")
                    cur.copy_from(output, self.table_name, null="")
                    
                conn.commit()

            self.log.info(f"Successfully loaded data into table {self.table_name}")

        except Exception as e:
            self.log.error(f"An error occurred while loading transformation data: {str(e)}")
            raise
