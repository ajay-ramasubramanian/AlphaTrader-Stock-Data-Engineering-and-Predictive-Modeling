from  pathlib import Path
import sys
project_root = Path(__file__).parents[2]
sys.path.append(str(project_root))
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.plugins.load_dim_operator import LoadDimOperator
from airflow.plugins.load_fact_operator import LoadFactOperator
from airflow.plugins.load_transformation_operator import LoadTransformationOperator

def initialize_python_operator(task_type,dag, task_name, callable_func):
        if not callable(callable_func):
            raise ValueError(f"The provided {task_type} function for {task_name} is not callable")
        return PythonOperator(
            task_id=f'{task_type}_{task_name}',
            python_callable=callable_func,
            dag=dag
        )

def initialize_postgres_operator(table_name, dag, postgres_conn_id, sql_query):
        return PostgresOperator(
        task_id=f"{table_name}_table",
        dag=dag,
        postgres_conn_id=postgres_conn_id,
        sql=sql_query
    )

def initialize_load_dim_operator(dag, topic, table_name, append):
        return LoadDimOperator(
            task_id=f"load_{table_name}_table",
            dag=dag,
            topic=topic,
            table_name=table_name,
            append=append
        )

def initialize_load_fact_operator(dag, topic, table_name):
        return LoadFactOperator(
            task_id=f"load_{table_name}_table",
            dag=dag,
            topic=topic,
            table_name=table_name,
        )
    
def initialize_load_transformation_operator(dag, topic, table_name, key):
        return LoadTransformationOperator(
            task_id=f'load_{table_name}_table',
            dag=dag,
            topic=topic,
            table_name=table_name,
            key=key
        )

