from  pathlib import Path
import sys
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))
from airflow.utils.task_group import TaskGroup
from dags.operators import initialize_python_operator
from dags.utils import process_to_presentation_task_configs


def move_to_presentation_group(dag):
    with TaskGroup('move_to_presentation_group') as group:
            move_to_presentation_tasks = [initialize_python_operator(
                dag, 'ingestion', name, config) for name, config in process_to_presentation_task_configs.items()]
    return group