"""Déclenchez les DAGs #1 et #2 et faites quelque chose s'ils réussissent."""

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago


with DAG(
    'Dag_master',
    schedule_interval='*/1 * * * *', # Chaque minute
    start_date=days_ago(0),
    catchup=False) as dag:
  def greeting():
    """Vérifiez simplement que le DAG a démarré dans le journal."""
    import logging
    logging.info('Hello World from DAG MASTER')

  externalsensor1 = ExternalTaskSensor(
    task_id='dag_1_completed_status',
    external_dag_id='dag_1',
    external_task_id=None, #Attendez que tout le DAG soit terminé
    check_existence=True,
    timeout=120)

  externalsensor2 = ExternalTaskSensor(
    task_id='dag_2_completed_status',
    external_dag_id='dag_2',
    external_task_id=None, # Attendez que tout le DAG soit terminé
    check_existence=True,
    timeout=120)

  goodbye_dummy = DummyOperator(task_id='goodbye_master')

  [externalsensor1, externalsensor2] >> goodbye_dummy
