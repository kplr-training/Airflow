from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from kombu import Connection, Exchange, Queue, Consumer, Producer
import logging

def write_to_rabbitmq():
    try:
        with Connection('amqp://defaultuser:defaultpassword@rabbitmq/') as conn:
            queue = Queue('myqueue', exchange=Exchange('myexchange', type='direct'), routing_key='mykey')
            with conn.channel() as channel:
                producer = Producer(channel)
                message = {
                    'timestamp': str(datetime.now()),
                    'data': 'Hello, RabbitMQ!'
                }
                producer.publish(message, exchange=queue.exchange, routing_key=queue.routing_key)
                logging.info(f"Message sent to RabbitMQ: {message}")
    except Exception as e:
        logging.error(f"Error sending message to RabbitMQ: {e}")


dag = DAG('rabbitmq_write', description='A simple Airflow DAG that writes messages to RabbitMQ queue',
          schedule_interval=None,
          start_date=datetime(2023, 4, 12),
          catchup=False)

write_task = PythonOperator(
    task_id='write_to_rabbitmq',
    python_callable=write_to_rabbitmq,
    dag=dag
)


write_task 