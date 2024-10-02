from  pathlib import Path
import sys
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))
from airflow.utils.task_group import TaskGroup
from dags.operators import initialize_python_operator
from dags.utils import independent_ingestion_task_configs , dependent_ingestion_task_configs

def independent_ingestion_group(dag):
    with TaskGroup('independent_ingestion_group') as group:
        independent_ingestion_tasks = [initialize_python_operator(
            'ingestion',dag, name, config) for name, config in independent_ingestion_task_configs.items()]
    return group

def dependent_ingestion_group(dag):
    with TaskGroup('dependent_ingestion_group') as group:
        dependent_ingestion_tasks = [initialize_python_operator(
            'ingestion',dag, name, config) for name, config in dependent_ingestion_task_configs.items()]
    return group