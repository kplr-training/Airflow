from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import random
from datetime import datetime

with DAG('const_plus_single_param', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    @task
    def generate_files():
        return [f"file_{nb}" for nb in range(random.randint(2, 5))]

    @task
    def download_file(path: str, file: str):
        return (f"echo '{path}/{file}' >> /tmp/out-const.txt")

    print_file = BashOperator.partial(task_id="print_file", do_xcom_push=False).expand(
    bash_command=download_file.partial(path="files/partner").expand(file=generate_files()))

    # Define the task that consumes the dataset
    check_files = BashOperator(
        task_id="check_files",
        bash_command="cat /tmp/out-const.txt",
        retries=3,
    )

    print_file >> check_files
