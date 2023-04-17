import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    'write_to_postgres_ux',
    start_date=datetime(2023, 4, 9),
    schedule_interval='@daily'
)

def write_to_postgres():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS my_test_table (
            col1 TEXT,
            col2 TEXT
        )
    """)
    cur.execute("INSERT INTO my_test_table (col1, col2) VALUES ('value1', 'value2')")
    conn.commit()
    cur.close()
    conn.close()

write_task = PythonOperator(
    task_id='write_task',
    python_callable=write_to_postgres,
    dag=dag
)
