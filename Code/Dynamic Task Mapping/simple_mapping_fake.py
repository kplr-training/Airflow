from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG('single_map', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    @task
    def download_file(file: str):
        download(file)

    files = download_file.expand(file=["file_a", "file_b", "file_c"])
