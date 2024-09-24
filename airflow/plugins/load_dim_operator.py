from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import psycopg2
from io import StringIO

class LoadDimOperator(BaseOperator):

    @apply_defaults
    def __init__(self, df, table_name, append=True, *args, **kwargs):

        self.conn = psycopg2.connect("dbname=spotify_db user=spotify password=spotify_pass host=postgres-warehouse")
        self.cur = self.conn.cursor()
        self.df = df
        self.table_name = table_name
        self.append = append


    def execute(self,):
        try:
            output = StringIO()
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