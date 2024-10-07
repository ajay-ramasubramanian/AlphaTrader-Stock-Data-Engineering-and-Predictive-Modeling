from pathlib import Path
import os
import sys

# Set the project root directory and add it to system path for importing modules
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

from airflow.utils.task_group import TaskGroup
from dags.operators import initialize_postgres_operator, initialize_load_dim_operator, \
                            initialize_load_fact_operator, initialize_load_transformation_operator
from dags.utils import create_table_task_configs, insert_to_dim_table_task_configs, \
                        insert_to_fact_table_task_configs, insert_to_transformation_table_task_configs

# Function to create the database schema
def create_database_schema(dag, schema):
    task_id = 'create_schema'  # Define task ID
    conn_id = os.environ.get('POSTGRES_CONN_ID')  # Fetch connection ID from environment variables
    sql_query = f"""
        SET search_path TO {schema};
        """  # SQL query to set the search path to the given schema
    
    return initialize_postgres_operator(dag, task_id, conn_id, sql_query)  # Initialize the Postgres operator with the query

# Function to create task group for table creation
def create_table_group(dag):
    with TaskGroup('create_table_group') as group:
        # Create Postgres operator tasks for each table creation query
        create_tables_tasks = [initialize_postgres_operator(
            dag, table_name=table_name, postgres_conn_id='postgres-warehouse', sql_query=sql_query
        ) for table_name, sql_query in create_table_task_configs.items()]
    return group  # Return the task group

# Function to create task group for loading dimension tables
def load_dimension_group(dag):
    with TaskGroup('load_dimension_group') as group:
        # Create load dimension operator tasks for each dimension table
        load_dim_tasks = [initialize_load_dim_operator(
            dag, topic=topic, table_name=table_name, append=False
        ) for table_name, topic in insert_to_dim_table_task_configs.items()]
    return group  # Return the task group

# Function to create task group for loading fact tables
def load_fact_group(dag):
    with TaskGroup('load_fact_group') as group:
        # Create load fact operator tasks for each fact table
        load_fact_tasks = [initialize_load_fact_operator(
            dag, topic=topic, table_name=table_name
        ) for table_name, topic in insert_to_fact_table_task_configs.items()]
    return group  # Return the task group

# Function to create task group for loading transformation tables
def load_transformation_group(dag):
    with TaskGroup('load_transformation_group') as group:
        # Create load transformation operator tasks for each transformation table
        load_transformation_tasks = [initialize_load_transformation_operator(
            dag, topic=config['topic'], table_name=table_name, key=config['key']
        ) for table_name, config in insert_to_transformation_table_task_configs.items()]
    return group  # Return the task group
