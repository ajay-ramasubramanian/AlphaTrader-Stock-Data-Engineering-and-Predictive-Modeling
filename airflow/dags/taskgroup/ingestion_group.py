from pathlib import Path
import sys

# Set the project root directory and add it to system path for importing modules
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

from airflow.utils.task_group import TaskGroup
from dags.operators import initialize_python_operator
from dags.utils import independent_ingestion_task_configs, dependent_ingestion_task_configs

# Function to create task group for independent ingestion tasks
def independent_ingestion_group(dag):
    with TaskGroup('independent_ingestion_group') as group:
        # Create Python operator tasks for each independent ingestion configuration
        independent_ingestion_tasks = [initialize_python_operator(
            dag, 'ingestion', name, config) for name, config in independent_ingestion_task_configs.items()]
    return group  # Return the task group

# Function to create task group for dependent ingestion tasks
def dependent_ingestion_group(dag):
    with TaskGroup('dependent_ingestion_group') as group:
        # Create Python operator tasks for each dependent ingestion configuration
        dependent_ingestion_tasks = [initialize_python_operator(
            dag, 'ingestion', name, config) for name, config in dependent_ingestion_task_configs.items()]
    return group  # Return the task group
