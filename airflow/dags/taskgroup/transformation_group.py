from  pathlib import Path
import sys
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))
from airflow.utils.task_group import TaskGroup
from dags.operators import initialize_python_operator
from dags.utils import transformation_task_configs

def transformation_group(dag):
    with TaskGroup('transformation_group') as group:
        transformation_tasks = [initialize_python_operator(
            dag, 'transformation', name, config) for name, config in transformation_task_configs.items()]
    return group