from  pathlib import Path
import sys
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from dags.operators import initialize_data_quality_operator
from dags.utils import insert_to_dim_table_task_configs, insert_to_fact_table_task_configs, create_data_checks_task_configs
from dags.operators import initialize_python_operator

schema = 'spotify'

def initialize_expectation_suites(dag):

    with TaskGroup('initialize_expectation_suites') as group:  
        create_expectation_suites = [initialize_python_operator(
            dag, task_type='data_quality', task_name=task_name, callable_func=func
        ) for task_name, func in create_data_checks_task_configs.items()]

    return group

def dimension_check_group():
    with TaskGroup('dimension_table_checks') as group:
        dim_tables = list(insert_to_dim_table_task_configs.keys())
        dim_tasks = [initialize_data_quality_operator(table, schema) for table in dim_tables]

    return group

def fact_check_group():
    with TaskGroup('fact_table_checks') as group:
        fact_tables = list(insert_to_fact_table_task_configs.keys())
        fact_tasks = [initialize_data_quality_operator(table, schema) for table in fact_tables]

    return group