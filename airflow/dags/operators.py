from pathlib import Path
import sys
import os

# Add the project root directory to the system path for module imports
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

# Import required operators and plugins for Airflow tasks
from airflow.operators.python import PythonOperator  # For Python task execution
from airflow.operators.postgres_operator import PostgresOperator  # For executing SQL queries in Postgres
from airflow.plugins.load_dim_operator import LoadDimOperator  # Custom operator for loading dimension tables
from airflow.plugins.load_fact_operator import LoadFactOperator  # Custom operator for loading fact tables
from airflow.plugins.load_transformation_operator import LoadTransformationOperator  # Custom operator for loading transformation tables
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator  # Operator for data validation

# Function to initialize a PythonOperator for running Python callable tasks
def initialize_python_operator(dag, task_type, task_name, callable_func):
    if not callable(callable_func):
        raise ValueError(f"The provided {task_type} function for {task_name} is not callable")  # Validate the callable
    return PythonOperator(
        dag=dag,  # Attach the operator to the DAG
        task_id=f'{task_type}_{task_name}',  # Define a unique task ID based on the task type and name
        python_callable=callable_func,  # The Python function to be executed
    )

# Function to initialize a PostgresOperator for executing SQL queries
def initialize_postgres_operator(dag, table_name, postgres_conn_id, sql_query):
    return PostgresOperator(
        dag=dag,  # Attach the operator to the DAG
        task_id=f"{table_name}",  # Define a task ID based on the table name
        postgres_conn_id=postgres_conn_id,  # Connection ID for the Postgres database
        sql=sql_query  # SQL query to be executed
    )

# Function to initialize the custom LoadDimOperator for loading data into dimension tables
def initialize_load_dim_operator(dag, topic, table_name, append):
    return LoadDimOperator(
        dag=dag,  # Attach the operator to the DAG
        task_id=f"load_{table_name}_table",  # Define a task ID based on the table name
        topic=topic,  # Topic of data to be loaded
        table_name=table_name,  # Table name for loading the data
        append=append  # Whether to append data to the table or overwrite
    )

# Function to initialize the custom LoadFactOperator for loading data into fact tables
def initialize_load_fact_operator(dag, topic, table_name):
    return LoadFactOperator(
        dag=dag,  # Attach the operator to the DAG
        task_id=f"load_{table_name}_table",  # Define a task ID based on the table name
        topic=topic,  # Topic of data to be loaded
        table_name=table_name,  # Table name for loading the data
    )

# Function to initialize the custom LoadTransformationOperator for loading transformation tables
def initialize_load_transformation_operator(dag, topic, table_name, key):
    return LoadTransformationOperator(
        dag=dag,  # Attach the operator to the DAG
        task_id=f'load_{table_name}_table',  # Define a task ID based on the table name
        topic=topic,  # Topic of data to be loaded
        table_name=table_name,  # Table name for loading the data
        key=key  # Key for transformations
    )

# Function to initialize the GreatExpectationsOperator for data quality checks
def initialize_data_quality_operator(table_name: str, schema):
    return GreatExpectationsOperator(
        conn_id=os.environ.get('POSTGRES_CONN_ID'),  # Connection ID for Postgres
        schema="public",  # Schema to perform data quality checks
        data_asset_name=f"{schema}.{table_name}",  # Asset name for the data table
        task_id=f"validate_{table_name}",  # Task ID based on the table name
        data_context_root_dir="/opt/airflow/projects/gx",  # Directory where Great Expectations configurations are stored
        expectation_suite_name=f"{table_name}_suite",  # Name of the expectation suite for data validation
        return_json_dict=True,  # Return results as a JSON dictionary
        fail_task_on_validation_failure=True  # Fail the task if validation fails
    )
