from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator, DummyOperator, LoadDimOperator, LoadFactOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pendulum
from airflow.utils.task_group import TaskGroup
import sys
from pathlib import Path


# Add the project root to the Python path
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

# import os
# print("Current working directory:", os.getcwd())
# print("Python path:", sys.path)
from ingestion.get_following_artist import run_retrieve_following_artists
from ingestion.get_liked_songs import run_retrieve_liked_songs
from ingestion.get_recent_plays import run_retrieve_recent_plays
from ingestion.get_saved_playlist import run_retrieve_saved_playlist
from ingestion.get_top_artists  import run_retrieve_top_artists
from ingestion.get_top_songs import run_retrieve_top_songs
import sql_queries
from ingestion.get_artist_albums import run_get_user_artist_albums
from ingestion.get_related_artists import run_get_artist_related_artists

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'spotify_pipeline_dag',
    default_args=default_args,
    description='Spotify ETL with Airflow',
    schedule=None,
    start_date= pendulum.today('UTC').add(days=-1),
    catchup=False,
) as dag:
    
#   PRODUCER SCRIPTS
    ingestion_task_configs = {
    'following_artists': run_retrieve_following_artists,
    'liked_songs': run_retrieve_liked_songs,
    'recent_plays': run_retrieve_recent_plays,
    'saved_playlists': run_retrieve_saved_playlist,
    'top_songs': run_retrieve_top_songs,
    'top_artists': run_retrieve_top_artists,
    'artist_albums': run_get_user_artist_albums,
    'related_artists': run_get_artist_related_artists
}

    create_table_task_config = {
        "artist": sql_queries.create_artist_table,
        "album": sql_queries.create_albums_table,
        "time": sql_queries.create_time_table,
        "track": sql_queries.create_tracks_table,
        "liked_songs": sql_queries.create_liked_songs_table
    }

    tables_to_insert = []

    
    def initialize_python_operator(task_type, task_name, callable_func):
        if not callable(callable_func):
            raise ValueError(f"The provided {task_type} function for {task_name} is not callable")
        return PythonOperator(
            task_id=f'{task_type}_{task_name}',
            python_callable=callable_func,
            dag=dag
        )

    def initialize_postgres_operator(table_name, dag, postgres_conn_id, sql_query):
        return PostgresOperator(
        task_id=f"create_{table_name}_table",
        dag=dag,
        postgres_conn_id=postgres_conn_id,
        sql=sql_query
    )

    def initialize_load_dim_operator():
        return LoadDimOperator(
            task_id=None,
            dag=dag,
            df=None,
            table_name=None,
            append=None
        )

    def initialize_load_fact_operator():
        return LoadFactOperator(
            task_id=None,
            dag=dag,
            df=None,
            table_name=None
        )


    start_operator = DummyOperator(task_id='Start_execution',  dag=dag)
    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
        
    
    with TaskGroup('ingestion_group') as ingestion_group:
        ingestion_tasks = [initialize_python_operator('ingestion', name, 
                                config) for name, config in ingestion_task_configs.items()]

        
    with TaskGroup('create_table_group') as create_table_group:
        create_tables_tasks = [initialize_postgres_operator(
            table_name=table_name, dag=dag, postgres_conn_id='postgres-warehouse', sql_query=sql_query
        ) for table_name, sql_query in create_table_task_config.items()]

    
    # to_warehouse = initialize_python_operator('load_to_warehouse', 'all_tracks', gold_to_warehouse)

    start_operator >> ingestion_group >> create_table_group >> end_operator
