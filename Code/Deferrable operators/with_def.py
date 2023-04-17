from pendulum import datetime
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensorAsync

with DAG(
    "async_dag_2",
    start_date=datetime(2023, 4, 8, 15, 30),
    end_date=datetime(2023, 4, 8, 15, 40),
    schedule="* * * * *",
    catchup=True,
) as dag:
    async_sensor = DateTimeSensorAsync(
        task_id="async_task",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=10) }}""",
    )