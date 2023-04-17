from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from kombu import Connection, Exchange, Queue, Consumer, Producer
import logging

def read_from_rabbitmq():
    try:
        with Connection('amqp://defaultuser:defaultpassword@rabbitmq/') as conn:
            queue = Queue('myqueue', exchange=Exchange('myexchange', type='direct'), routing_key='mykey')
            with conn.channel() as channel:
                consumer = Consumer(channel, queues=queue, callbacks=[process_message])
                consumer.consume()
                conn.drain_events(timeout=10)
    except Exception as e:
        logging.error(f"Error receiving message from RabbitMQ: {e}")

def process_message(body, message):
    logging.info(f"Received message from RabbitMQ: {body}")
    message.ack()

dag = DAG('rabbitmq_read', description='A simple Airflow DAG that reads messages from a RabbitMQ queue',
          schedule_interval=None,
          start_date=datetime(2023, 4, 12),
          catchup=False)

read_task = PythonOperator(
    task_id='read_from_rabbitmq',
    python_callable=read_from_rabbitmq,
    execution_timeout=timedelta(seconds=15),
    dag=dag
)

read_task
