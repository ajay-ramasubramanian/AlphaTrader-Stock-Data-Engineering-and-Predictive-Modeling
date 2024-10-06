# Import necessary libraries for the DAG and Airflow operators
from airflow import DAG
import os
from airflow.decorators import dag, task  # Allows defining DAGs and tasks using decorators
import sys
from pathlib import Path

# Add the project root directory to the system path for module imports
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))

# Import additional libraries and operators required for the DAG
from datetime import timedelta
import pendulum  # Provides enhanced date and time manipulation
from airflow.operators.dummy_operator import DummyOperator  # For placeholder start and end tasks
from airflow.utils.task_group import TaskGroup  # Helps group related tasks logically
# Import custom task groups from external modules
from taskgroup.ingestion_group import (dependent_ingestion_group, independent_ingestion_group)
from taskgroup.presentation_group import move_to_presentation_group
from taskgroup.table_group import (create_database_schema, create_table_group, load_dimension_group,
                                   load_fact_group, load_transformation_group)
from taskgroup.transformation_group import transformation_group
from taskgroup.data_quality import (initialize_expectation_suites, dimension_check_group, fact_check_group)

# Define default arguments for the DAG such as retries, owner, and email notifications
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Ensure each DAG run is independent of previous ones
    'email_on_failure': False,  # Disable email notifications on failure
    'email_on_retry': False,    # Disable email notifications on retry
    'retries': 1,               # Number of retry attempts on failure
    'retry_delay': timedelta(minutes=5),  # Delay between retry attempts
}

# Define the schema for the database used in this pipeline
database_schema = "public"

# Create the DAG for the Spotify ETL pipeline
with DAG(
    'spotify_pipeline_dag',  # DAG name
    default_args=default_args,  # Attach default arguments
    description='Spotify ETL with Airflow',  # Description for the DAG
    schedule=None,  # No regular schedule for this DAG
    dagrun_timeout=timedelta(minutes=200),  # Timeout for the DAG run
    start_date=pendulum.today('UTC').add(days=-1),  # Set start date to 1 day in the past
    catchup=False,  # Disable backfilling of missed runs
) as dag:

    # Define start and end tasks using DummyOperator as placeholders
    start_operator = DummyOperator(task_id='Start_execution')
    end_operator = DummyOperator(task_id='Stop_execution')

    # Create tasks for schema creation, data quality checks, and data ingestion
    create_schema = create_database_schema(dag, schema=database_schema)  # Create the database schema
    expectation_suites = initialize_expectation_suites(dag)  # Initialize data quality checks
    independent_ingestion = independent_ingestion_group(dag)  # Run independent data ingestion tasks
    dependent_ingestion = dependent_ingestion_group(dag)  # Run dependent data ingestion tasks
    move_to_presentation = move_to_presentation_group(dag)  # Move data to the presentation layer
    transformation = transformation_group(dag)  # Apply data transformations
    create_table = create_table_group(dag)  # Create tables in the database
    load_dimension = load_dimension_group(dag)  # Load data into dimension tables
    load_fact = load_fact_group(dag)  # Load data into fact tables
    load_transformation = load_transformation_group(dag)  # Load data into transformation tables
    dimension_checks = dimension_check_group()  # Perform data quality checks on dimension tables
    fact_checks = fact_check_group()  # Perform data quality checks on fact tables

    # Define task dependencies and execution order for the DAG
    start_operator >> \
    expectation_suites >> \
    independent_ingestion >> dependent_ingestion >> \
    move_to_presentation >> transformation >>  \
    create_schema >> create_table >> \
    load_dimension >> dimension_checks >> \
    load_fact >> fact_checks \
    >> load_transformation >> end_operator
