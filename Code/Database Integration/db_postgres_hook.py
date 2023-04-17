from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    'postgres_hook_example',
    default_args=default_args,
    schedule_interval='@daily'
) 

# Define the PythonOperator
def run_query():
    # Create a PostgresHook
    hook = PostgresHook(postgres_conn_id='MY_DATABASE_CONN_0')
    
    # Execute a query
    
    query = "SELECT * FROM my_test_table"
    rows = hook.get_records(query)
    for row in rows:
            print(row)


task = PythonOperator(
    task_id='run_query',
    python_callable=run_query,
    dag=dag
)
