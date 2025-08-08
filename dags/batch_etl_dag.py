from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=0),
}

with DAG('batch_etl_dag', default_args=default_args, schedule_interval='*/10 * * * *', catchup=False) as dag:
    run_etl = BashOperator(
        task_id="run_batch_etl",
        bash_command="""
            docker exec final_project-spark-master-1 /opt/bitnami/spark/bin/spark-submit \
            --packages mysql:mysql-connector-java:8.0.33 \
            --master spark://spark-master:7077 \
            /app/batch_etl.py
        """
    )
