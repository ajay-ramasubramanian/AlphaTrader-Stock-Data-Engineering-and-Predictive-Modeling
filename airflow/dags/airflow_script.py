from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pendulum
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
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
from consumers.following_artists_consumer import run_consumer_following_artists
from consumers.liked_songs_consumer import run_consumer_liked_songs
from consumers.recent_plays_consumer import run_consumer_recently_played
from consumers.saved_playlists_consumer import run_consumer_saved_playlist
from consumers.top_artists_consumer import run_consumer_top_artists
from consumers.top_songs_consumer import run_consumer_top_songs
from producers.produce_following_artists import run_producer_following_artists
from producers.produce_liked_songs import run_producer_liked_songs
from producers.produce_recent_plays import run_producer_recent_plays
from producers.produce_saved_playlists import run_producer_saved_playlist
from producers.produce_top_artists import run_producer_top_artist
from producers.produce_top_songs import run_producer_top_tracks
# print("Importing ingestion modules...")
# try:
#     from ingestion.get_following_artist import run_retrieve_following_artists
#     print("Ingestion import successful")
# except Exception as e:
#     print(f"Error importing ingestion module: {e}")

# print("Importing consumer modules...")
# try:
#     from consumers.following_artists_consumer import run_consumer_following_artists
#     print("Consumer import successful")
# except Exception as e:
#     print(f"Error importing consumer module: {e}")

# print("Importing producer modules...")
# try:
#     from producers.produce_following_artists import run_producer_following_artists
#     print("Producer import successful")
# except Exception as e:
#     print(f"Error importing producer module: {e}")

# print("All imports completed")


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
    'kafka_ingestion_dag',
    default_args=default_args,
    description='Kafka to ingestion pipeline with Airflow',
    schedule=None,
    start_date= pendulum.today('UTC').add(days=-1),
    catchup=False,
) as dag:
    
#   PRODUCER SCRIPTS
    
    task_configs = {
        'following_artists': {
            'producer': run_producer_following_artists,
            'consumer': run_consumer_following_artists,
            'ingestion': run_retrieve_following_artists
        },
        # 'liked_songs': {
        #     'producer': run_producer_liked_songs,
        #     'consumer': run_consumer_liked_songs,
        #     'ingestion': run_retrieve_liked_songs
        # },
        # 'recent_plays': {
        #     'producer': run_producer_recent_plays,
        #     'consumer': run_consumer_recently_played,
        #     'ingestion': run_retrieve_recent_plays
        # },
        # 'saved_playlists': {
        #     'producer': run_producer_saved_playlist,
        #     'consumer': run_consumer_saved_playlist,
        #     'ingestion': run_retrieve_saved_playlist
        # },
        # 'top_songs': {
        #     'producer': run_producer_top_tracks,
        #     'consumer': run_consumer_top_songs,
        #     'ingestion': run_retrieve_top_songs
        # },
        # 'top_artists': {
        #     'producer': run_producer_top_artist,
        #     'consumer': run_consumer_top_artists,
        #     'ingestion': run_retrieve_top_artists
        # }
    }

    
    def create_python_operator(task_type, task_name, callable_func):
        if not callable(callable_func):
            raise ValueError(f"The provided {task_type} function for {task_name} is not callable")
        return PythonOperator(
            task_id=f'{task_type}_{task_name}',
            python_callable=callable_func,
            dag=dag
        )

    # Create tasks using list comprehension
    producer_tasks = [create_python_operator('produce', name, config['producer']) for name, config in task_configs.items()]
    consumer_tasks = [create_python_operator('consumer', name, config['consumer']) for name, config in task_configs.items()]
    ingestion_tasks = [create_python_operator('ingestion', name, config['ingestion']) for name, config in task_configs.items()]
    

    stop_kafka_zookeeper = BashOperator(
    task_id='stop_kafka_zookeeper',
    bash_command='docker-compose stop kafka zookeeper',
    )

    chain (producer_tasks)



# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2023, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def print_hello():
#     return 'Hello from Airflow!'

# dag = DAG(
#     'test_dag',
#     default_args=default_args,
#     description='A simple test DAG',
#     schedule=timedelta(days=1),
# )

# t1 = PythonOperator(
#     task_id='print_hello',
#     python_callable=print_hello,
#     dag=dag,
# )

# if __name__ == "__main__":
#     dag.cli()