from pathlib import Path
import sys

# Set the project root directory and add it to system path for importing modules
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from dags.operators import initialize_data_quality_operator
from dags.utils import insert_to_dim_table_task_configs, insert_to_fact_table_task_configs, create_data_checks_task_configs
from dags.operators import initialize_python_operator

schema = 'public'

# Function to initialize expectation suites for data quality checks
def initialize_expectation_suites(dag):
    # Create a task group for initializing Great Expectations suites for data validation
    with TaskGroup('initialize_expectation_suites') as group:  
        create_expectation_suites = [initialize_python_operator(
            dag, task_type='data_quality', task_name=task_name, callable_func=func
        ) for task_name, func in create_data_checks_task_configs.items()]  # Create tasks for each data check
    return group  # Return the task group

def dimension_check_group(dag):
    with TaskGroup('dimension_table_checks') as group:
        # Retrieve list of dimension tables and initialize data quality tasks for each
        dim_tables = list(insert_to_dim_table_task_configs.keys())
        dim_tasks = [initialize_data_quality_operator(table, schema) for table in dim_tables]
    return group  # Return the task group

    return group

def fact_check_group(dag):
    with TaskGroup('fact_table_checks') as group:
        # Retrieve list of fact tables and initialize data quality tasks for each
        fact_tables = list(insert_to_fact_table_task_configs.keys())
        fact_tasks = [initialize_data_quality_operator(table, schema) for table in fact_tables]
    return group  # Return the task group
