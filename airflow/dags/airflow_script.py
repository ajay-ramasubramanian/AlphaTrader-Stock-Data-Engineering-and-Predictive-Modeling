import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

from datetime import timedelta

import pendulum
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from taskgroup.ingestion_group import (dependent_ingestion_group,
                                    independent_ingestion_group)
from taskgroup.presentation_group import move_to_presentation_group
from taskgroup.table_group import (create_table_group, load_dimension_group,
                                load_fact_group, load_transformation_group)
from taskgroup.transformation_group import transformation_group

from airflow import DAG

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
    
    start_operator = DummyOperator(task_id='Start_execution',  dag=dag)
    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
    
    independent_ingestion = independent_ingestion_group(dag)
    dependent_ingestion = dependent_ingestion_group(dag)
    move_to_presentation = move_to_presentation_group(dag)
    transformation = transformation_group(dag)
    create_table = create_table_group(dag)
    load_dimension = load_dimension_group(dag)
    load_fact = load_fact_group(dag)
    load_transformation = load_transformation_group(dag)
    

    start_operator >> \
    independent_ingestion >> \
    dependent_ingestion >> \
    move_to_presentation >> \
    transformation >> \
    create_table >> \
    load_dimension >> \
    load_fact >> \
    load_transformation >> \
    end_operator

    # create_table >> load_fact