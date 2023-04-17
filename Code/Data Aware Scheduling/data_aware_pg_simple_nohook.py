import psycopg2
from datetime import datetime
from airflow import DAG, Dataset
from airflow.operators.python_operator import PythonOperator

pg_dataset=[Dataset("postgres://airflow:airflow@postgres:5432/airflow?table=my_test_table")]

with DAG(
    'write_to_postgres_ux',
    start_date=datetime(2023, 4, 9),
    schedule_interval='@daily'
)as dag:

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
        outlets=pg_dataset,
        python_callable=write_to_postgres,
        dag=dag
    )

with DAG(
    'read_from_postgres_ux',
    start_date=datetime(2023, 1, 1),
    schedule = pg_dataset
) as dag:

    def read_from_postgres():
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cur = conn.cursor()
        cur.execute("SELECT col1, col2 FROM my_test_table")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        cur.close()
        conn.close()

    read_task = PythonOperator(
        task_id='read_task',
        python_callable=read_from_postgres,
        dag=dag
    )
