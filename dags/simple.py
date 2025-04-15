from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG("hello_world",
         start_date=datetime(2023, 1, 1),
         schedule_interval="@daily",
         catchup=False) as dag:
    start = DummyOperator(task_id="start")
