from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.plugins.load_dim_operator import LoadDimOperator
from airflow.plugins.load_fact_operator import LoadFactOperator
from airflow.plugins.load_transformation_operator import LoadTransformationOperator
from datetime import timedelta
import pendulum
from airflow.utils.task_group import TaskGroup
import sys
from pathlib import Path

from utils import (independent_ingestion_task_configs, dependent_ingestion_task_configs, 
                process_to_presentation_task_configs, transformation_task_configs, 
                create_table_task_configs, insert_to_dim_table_task_configs, insert_to_transformation_table_task_configs,
                insert_to_fact_table_task_configs)


# Add the project root to the Python path
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

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
        task_id=f"{table_name}_table",
        dag=dag,
        postgres_conn_id=postgres_conn_id,
        sql=sql_query
    )

    def initialize_load_dim_operator(dag, topic, table_name, append):
        return LoadDimOperator(
            task_id=f"load_{table_name}_table",
            dag=dag,
            topic=topic,
            table_name=table_name,
            append=append
        )

    def initialize_load_fact_operator(dag, topic, table_name):
        return LoadFactOperator(
            task_id=f"load_{table_name}_table",
            dag=dag,
            topic=topic,
            table_name=table_name,
        )
    
    def initialize_load_transformation_operator(dag, topic, table_name):
        return LoadTransformationOperator(
            task_id=f"load_{table_name}_table",
            dag=dag,
            topic=topic,
            table_name=table_name,
        )


    start_operator = DummyOperator(task_id='Start_execution',  dag=dag)
    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
        
    
    with TaskGroup('independent_ingestion_group') as independent_ingestion_group:
        independent_ingestion_tasks = [initialize_python_operator(
            'ingestion', name, config) for name, config in independent_ingestion_task_configs.items()]

    
    with TaskGroup('dependent_ingestion_group') as dependent_ingestion_group:
        dependent_ingestion_tasks = [initialize_python_operator(
            'ingestion', name, config) for name, config in dependent_ingestion_task_configs.items()]
        
    
    with TaskGroup('move_to_presentation_group') as move_to_presentation_group:
        move_to_presentation_tasks = [initialize_python_operator(
            'ingestion', name, config) for name, config in process_to_presentation_task_configs.items()]
        

    with TaskGroup('transformation_group') as transformation_group:
        transformation_tasks = [initialize_python_operator(
            'transformation', name, config) for name, config in transformation_task_configs.items()]

        
    with TaskGroup('create_table_group') as create_table_group:
        create_tables_tasks = [initialize_postgres_operator(
            table_name=table_name, dag=dag, postgres_conn_id='postgres-warehouse', sql_query=sql_query
        ) for table_name, sql_query in create_table_task_configs.items()]

        chain (*create_tables_tasks)


    
    with TaskGroup('load_dimension_group') as load_dimension_group:
        load_dim_tasks = [initialize_load_dim_operator(
            dag=dag, topic=topic, table_name=table_name, append=False
        ) for table_name, topic in insert_to_dim_table_task_configs.items()]

        chain (*load_dim_tasks)
        

    with TaskGroup('load_fact_group') as load_fact_group:
        load_fact_tasks = [initialize_load_fact_operator(
            dag=dag, topic=topic, table_name=table_name
        ) for table_name, topic in insert_to_fact_table_task_configs.items()]

        chain (*load_fact_group)
        

    with TaskGroup('load_transformation_group') as load_transformation_group:
        load_transformation_tasks = [initialize_load_transformation_operator(
             dag=dag, topic=topic, table_name=table_name
        ) for table_name, topic in insert_to_transformation_table_task_configs.items()]

        chain (*load_transformation_tasks)
    

    start_operator >> independent_ingestion_group >>dependent_ingestion_group >> \
    move_to_presentation_group >> \
    transformation_group  >>  create_table_group >> load_dimension_group \
    >> load_fact_group \
    >> load_transformation_group >> end_operator

    # load_dimension_group