from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from transformations.utils import MinioRetriever
import psycopg2
from io import StringIO

class LoadDimOperator(BaseOperator):

    @apply_defaults
    def __init__(self, topic, table_name, append=True, *args, **kwargs):
        super(LoadDimOperator, self).__init__(*args, **kwargs)

        self.df = MinioRetriever('suhaas', topic, 'presentation', 'minio').retrieve_object()
        self.conn = psycopg2.connect("dbname=spotify_db user=spotify password=spotify_pass host=postgres-warehouse")
        self.cur = self.conn.cursor()
        self.table_name = table_name
        self.append = append
        self.topic = topic


    def execute(self, context):
        try:
            output = StringIO()
            if self.topic == 'spotify-artist-albums':
                print(self.df[self.df['album_id'] == "1ntEaMBOvQQID1xN6HbZ2K"].T)
            self.df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)

            if not self.append:
                self.cur.execute(f"TRUNCATE TABLE {self.table_name}")
                
            self.cur.copy_from(output, self.table_name, null="")

        except Exception as e:
            print(f"Ohh no!! An exception has occured: {e}")

        finally:
            self.conn.commit()
            self.cur.close()
            self.conn.close()


# from airflow.models import BaseOperator
# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.exceptions import AirflowException
# from airflow.utils.decorators import apply_defaults

# from transformations.utils import MinioRetriever
# from io import StringIO

# class LoadDimOperator(BaseOperator):

#     @apply_defaults
#     def __init__(
#         self,
#         topic,
#         table_name,
#         postgres_conn_id='postgres-warehouse',
#         append=True,
#         *args, **kwargs
#     ):
#         super(LoadDimOperator, self).__init__(*args, **kwargs)
#         self.topic = topic
#         self.table_name = table_name
#         self.postgres_conn_id = postgres_conn_id
#         self.append = append

#     def execute(self, context):
#         self.log.info(f"Starting data load for table {self.table_name}")
        
#         try:
#             # Retrieve data from Minio
#             df = MinioRetriever('suhaas', self.topic, 'presentation', 'minio').retrieve_object()
            
#             # Get PostgresHook
#             pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
            
#             # Prepare data for insertion
#             output = StringIO()
#             df.to_csv(output, sep='\t', header=False, index=False)
#             output.seek(0)

#             # Use connection from the pool
#             with pg_hook.get_conn() as conn:
#                 with conn.cursor() as cur:
#                     if not self.append:
#                         self.log.info(f"Truncating table {self.table_name}")
#                         cur.execute(f"TRUNCATE TABLE {self.table_name}")
                    
#                     self.log.info(f"Copying data to table {self.table_name}")
#                     cur.copy_from(output, self.table_name, null="")
                
#                 # Commit the transaction
#                 conn.commit()

#             self.log.info(f"Data load completed for table {self.table_name}")

#         except Exception as e:
#             self.log.error(f"An error occurred while loading data to {self.table_name}: {e}")
#             raise AirflowException(f"Data load failed for table {self.table_name}: {e}")