from airflow import DAG
from airflow.decorators import task
import random
from datetime import datetime

with DAG('multi_p', start_date=datetime(2023, 4, 8), schedule_interval='@daily', catchup=False) as dag:
    @task
    def generate_files():
        return [f"file_{nb}" for nb in range(random.randint(2, 5))]

    @task
    def download_file(path: str, file: str):
        print(f"{path}/{file}")

    files = download_file.expand(path=["dags/partner_a", "dags/partner_b"], file=generate_files())