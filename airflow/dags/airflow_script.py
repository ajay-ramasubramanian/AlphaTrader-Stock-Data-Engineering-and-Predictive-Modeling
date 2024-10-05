# from airflow import DAG
import os
# from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
import sys
from pathlib import Path


# Add the project root to the Python path
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

from datetime import timedelta
from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from taskgroup.ingestion_group import (dependent_ingestion_group,
                                    independent_ingestion_group)
from taskgroup.presentation_group import move_to_presentation_group
from taskgroup.table_group import (create_database_schema, create_table_group, load_dimension_group,
                                load_fact_group, load_transformation_group)
from taskgroup.transformation_group import transformation_group
from taskgroup.data_quality import (initialize_expectation_suites, dimension_check_group, fact_check_group)



# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

schema = "public"

# def get_configs():
#     from utils import (independent_ingestion_task_configs, dependent_ingestion_task_configs,
#                        process_to_presentation_task_configs, transformation_task_configs,
#                        create_table_task_configs, insert_to_dim_table_task_configs,
#                        insert_to_transformation_table_task_configs, insert_to_fact_table_task_configs)

#     return (independent_ingestion_task_configs, dependent_ingestion_task_configs,
#             process_to_presentation_task_configs, transformation_task_configs,
#             create_table_task_configs, insert_to_dim_table_task_configs,
#             insert_to_transformation_table_task_configs, insert_to_fact_table_task_configs)
    
# Define the DAG
# @dag (
#     'spotify_pipeline_dag',
#     default_args=default_args,
#     description='Spotify ETL with Airflow',
#     schedule=None,
#     dagrun_timeout=timedelta(minutes=200),
#     start_date= pendulum.today('UTC').add(days=-1),
#     catchup=False,
# ) 
# def spotify_pipeline():


    
    # @task
    # def process_task(task_name: str, callable_func: Callable):
    #     if not callable(callable_func):
    #         raise ValueError(f"The provided function for {task_name} is not callable")
    #     return callable_func()


    # def create_task_group(group_name: str, config: Dict, task_type: str):
    #     with TaskGroup(group_name) as group:
    #         for name, func in config.items():
    #             process_task.override(task_id=f"{task_type}_{name}")(
    #                 task_name=name,
    #                 callable_func=func
    #             )
    #     return group


    # independent_ingestion_task_configs, dependent_ingestion_task_configs, \
    # process_to_presentation_task_configs, transformation_task_configs, \
    # create_table_task_configs, insert_to_dim_table_task_configs, \
    # insert_to_transformation_table_task_configs, insert_to_fact_table_task_configs = get_configs()


    
    # independent_ingestion = create_task_group('independent_ingestion_group', independent_ingestion_task_configs, 'ingestion')
    # dependent_ingestion = create_task_group('dependent_ingestion_group', dependent_ingestion_task_configs, 'ingestion')
    # move_to_presentation = create_task_group('move_to_presentation_group', process_to_presentation_task_configs, 'move')
    # transformation = create_task_group('transformation_group', transformation_task_configs, 'transformation')
    
        
    # with TaskGroup('create_table_group') as create_table_group:
    #     create_tables_tasks = [initialize_postgres_operator(
    #         table_name=f"{schema}_{table_name}_table", postgres_conn_id='postgres-warehouse', sql_query=sql_query
    #     ) for table_name, sql_query in create_table_task_configs.items()]

    #     chain (*create_tables_tasks)

    
    # with TaskGroup('load_dimension_group') as load_dimension_group:
    #     from airflow.plugins.load_dim_operator import LoadDimOperator

    #     def initialize_load_dim_operator(topic, table_name, append):
    #         return LoadDimOperator(
    #             task_id=f"load_{table_name}_table",
    #             topic=topic,
    #             table_name=f"{schema}.{table_name}",
    #             append=append
    #         )

    #     load_dim_tasks = [initialize_load_dim_operator(
    #         topic=topic, table_name=table_name, append=False
    #     ) for table_name, topic in insert_to_dim_table_task_configs.items()]

    #     chain (*load_dim_tasks)
        

    # with TaskGroup('load_fact_group') as load_fact_group:
    #     from airflow.plugins.load_fact_operator import LoadFactOperator

    #     def initialize_load_fact_operator(topic, table_name):
    #         return LoadFactOperator(
    #             task_id=f"load_{table_name}_table",
    #             topic=topic,
    #             table_name=table_name,
    #         )


    #     load_fact_tasks = [initialize_load_fact_operator(
    #         topic=topic, table_name=table_name
    #     ) for table_name, topic in insert_to_fact_table_task_configs.items()]

    #     chain (*load_fact_tasks)

        

    # with TaskGroup('load_transformation_group') as load_transformation_group:
    #     from airflow.plugins.load_transformation_operator import LoadTransformationOperator

    #     def initialize_load_transformation_operator(topic, table_name, key):
            
    #         return LoadTransformationOperator(
    #             task_id=f'load_{table_name}_table',
    #             topic=topic,
    #             table_name=table_name,
    #             key=key
    #         )
    #     load_transformation_tasks = [initialize_load_transformation_operator(
    #         topic=config['topic'], table_name=table_name, key=config['key']
    #     ) for table_name, config in insert_to_transformation_table_task_configs.items()]

    #     chain (*load_transformation_tasks)
    
with DAG(
    'spotify_pipeline_dag',
    default_args=default_args,
    description='Spotify ETL with Airflow',
    schedule=None,
    dagrun_timeout=timedelta(minutes=200),
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
) as dag:
    
    start_operator = DummyOperator(task_id='Start_execution')
    end_operator = DummyOperator(task_id='Stop_execution')
    
    create_schema = create_database_schema(dag, schema=schema)
    expectation_suites = initialize_expectation_suites(dag)
    independent_ingestion = independent_ingestion_group(dag)
    dependent_ingestion = dependent_ingestion_group(dag)
    move_to_presentation = move_to_presentation_group(dag)
    transformation = transformation_group(dag)
    create_table = create_table_group(dag)
    load_dimension = load_dimension_group(dag)
    load_fact = load_fact_group(dag)
    load_transformation = load_transformation_group(dag)
    dimension_checks = dimension_check_group(dag)
    fact_checks = fact_check_group(dag)


    start_operator >> \
    expectation_suites >> \
    independent_ingestion >> dependent_ingestion >> \
    move_to_presentation >> transformation >>  \
    create_schema >> create_table >> \
    load_dimension >> dimension_checks >> \
    load_fact >> fact_checks \
    >> load_transformation >> end_operator

    
    

