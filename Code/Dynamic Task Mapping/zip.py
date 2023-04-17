from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import random
from datetime import datetime

with DAG('zip', start_date=datetime(2023, 4, 8), schedule_interval='@daily', catchup=False) as dag:
    @task
    def generate_paths():
        return ['dags/partner_d', 'dags/partner_e']

    @task
    def generate_files():
        return ['file_d', 'file_e']

    @task
    def aggregate(path: str, file: str):
        return list(zip(path, file))

    @task
    def download_file(filepath: str):
        print(filepath)

    aggregated = aggregate(path=generate_paths(), file=generate_files())
    download_file.expand(filepath=aggregated)