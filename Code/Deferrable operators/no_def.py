from pendulum import datetime
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensor

with DAG(
    "sync_dag_1",
    start_date=datetime(2023, 4, 8, 15, 30),
    end_date=datetime(2023, 4, 8, 15, 40),
    schedule="* * * * *",
    catchup=True,
) as dag:
    sync_sensor = DateTimeSensor(
        task_id="sync_task",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=10) }}""",
    )