from pathlib import Path
import sys

# Set the project root directory and add it to system path for importing modules
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

from airflow.utils.task_group import TaskGroup
from dags.operators import initialize_python_operator
from dags.utils import process_to_presentation_task_configs

# Function to create task group for moving data to presentation layer
def move_to_presentation_group(dag):
    with TaskGroup('move_to_presentation_group') as group:
        # Create Python operator tasks for each process-to-presentation configuration
        move_to_presentation_tasks = [initialize_python_operator(
            dag, 'ingestion', name, config) for name, config in process_to_presentation_task_configs.items()]
    return group  # Return the task group
