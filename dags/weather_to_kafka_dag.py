from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('weather_to_kafka_dag',
         default_args=default_args,
         schedule_interval='*/5 * * * *',  # every minute
         catchup=False) as dag:

    run_producer = BashOperator(
        task_id="run_weather_producer",
        bash_command="python /opt/airflow/kafka_producer/weather_to_kafka.py"
    )
