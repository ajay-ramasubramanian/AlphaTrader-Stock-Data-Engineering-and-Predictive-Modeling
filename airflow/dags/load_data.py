import sys
from pathlib import Path

project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

import pandas as pd
from transformations.utils import MinioRetriever
import psycopg2
from io import StringIO


def gold_to_warehouse():

    df = MinioRetriever('suhaas', 'spotify-all-tracks', 'processed', 'minio').retrieve_object()
    table_name = 'dim_track'

    import psycopg2
    from io import StringIO

    # Read parquet file from MinIO
    
    conn = psycopg2.connect("dbname=spotify_db user=spotify password=spotify_pass host=postgres-warehouse")
    cur = conn.cursor()

 
    output = StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)

    cur.copy_from(output, table_name, null="")

    conn.commit()
    cur.close()
    conn.close()

# gold_to_warehouse()