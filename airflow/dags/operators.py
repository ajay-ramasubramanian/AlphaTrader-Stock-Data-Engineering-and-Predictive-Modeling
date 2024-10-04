from  pathlib import Path
import sys
import os
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.plugins.load_dim_operator import LoadDimOperator
from airflow.plugins.load_fact_operator import LoadFactOperator
from airflow.plugins.load_transformation_operator import LoadTransformationOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator


def initialize_python_operator(dag, task_type, task_name, callable_func):
        if not callable(callable_func):
            raise ValueError(f"The provided {task_type} function for {task_name} is not callable")
        return PythonOperator(
            dag=dag,
            task_id=f'{task_type}_{task_name}',
            python_callable=callable_func,
        )

def initialize_postgres_operator(dag, table_name, postgres_conn_id, sql_query):

        return PostgresOperator(
        dag=dag,
        task_id=f"{table_name}",
        postgres_conn_id=postgres_conn_id,
        sql=sql_query
    )

def initialize_load_dim_operator(dag, topic, table_name, append):
        return LoadDimOperator(
            dag=dag,
            task_id=f"load_{table_name}_table",
            topic=topic,
            table_name=table_name,
            append=append
        )

def initialize_load_fact_operator(dag, topic, table_name):
        return LoadFactOperator(
               dag=dag,
            task_id=f"load_{table_name}_table",
            topic=topic,
            table_name=table_name,
        )
    
def initialize_load_transformation_operator(dag, topic, table_name, key):
        return LoadTransformationOperator(
            dag=dag,
            task_id=f'load_{table_name}_table',
            topic=topic,
            table_name=table_name,
            key=key
        )


def initialize_data_quality_operator(table_name: str, schema):
        return GreatExpectationsOperator(
            conn_id=os.environ.get('POSTGRES_CONN_ID'),
            schema=schema,
            data_asset_name=f"{schema}.{table_name}",
            task_id=f"validate_{table_name}",
            data_context_root_dir="gx",
            expectation_suite_name=f"{table_name}_suite",
            return_json_dict=True,
            fail_task_on_validation_failure=True
    )