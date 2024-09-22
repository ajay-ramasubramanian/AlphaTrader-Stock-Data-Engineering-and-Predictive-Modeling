import sys
from pathlib import Path

project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

# from sqlalchemy import create_engine
import pandas as pd
from transformations.utils import MinioRetriever


def gold_to_warehouse():

    df = MinioRetriever('suhaas', 'spotify-all-tracks', 'processed', 'minio').retrieve_object()
    

    df = df.drop_duplicates('track_id', keep='first')

    print(len(df['track_id']))
    print(len(df['track_id'].unique()))
    # # Create a connection to the PostgreSQL database
    # engine = create_engine('postgresql://spotify:spotify_pass@postgres-warehouse:5432/spotify_db')


    # # Or load data from CSV file (if you have CSV files)
    # # df = pd.read_csv('your_data.csv')

    # # Load the data into the PostgreSQL table
    table_name = 'dim_track'
    # # print(df.columns)

    # print("--------------------------------")
    # data = {
    #     'track_id': [1,2],
    #     'track_name': ['abcd', 'efgh'],
    #     'duration_ms': [222, 444], 
    #     'track_popularity': [92, 60], 
        
    #    'track_uri': ['hdgqwu', 'ajbhjdc'],
    #    'album_name': ['djwb', 'jewbf'],
    #    'artist_name': ['hdgqwu', 'ajbhjdc']
    # }
    # # print(df.head().to_dict())

    # df = pd.DataFrame(data)
    # # print(df)
    # # with engine.connect() as conn:
    # #     df.to_sql(table_name, con=conn.connection, if_exists='replace', index=False)

    # # print(f"Data loaded successfully into {table_name}")

    # from airflow.hooks.postgres_hook import PostgresHook

    # # In your DAG or operator
    # pg_hook = PostgresHook(postgres_conn_id='postgres-warehouse')
    # engine = pg_hook.get_sqlalchemy_engine()

    # # Now use this engine with pandas
    # df.to_sql(table_name, engine, if_exists='replace', index=False)

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