from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.branch_operator import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.subdag_operator import SubDagOperator

import random
import time


# Function to simulate data generation
def generate_data(*args, **kwargs):
    # Simulating data generation
    print("Generating data...")
    time.sleep(2)
    return random.choice([True, False])  # Random success/failure result


# Function to process data
def process_data(*args, **kwargs):
    print("Processing data...")
    time.sleep(2)


# Function for conditional branching
def branch_decision(*args, **kwargs):
    # Generate the random data again to decide which branch to take
    data = generate_data(*args, **kwargs)
    if data:
        return 'task_process_data'
    else:
        return 'task_failure'


# Task to handle failure scenario
def handle_failure(*args, **kwargs):
    print("Handling failure scenario...")


# Subdag for the processing step
def subdag_processing(parent_dag_name, child_dag_name, args):
    with DAG(
        dag_id=child_dag_name,
        default_args=args,
        schedule_interval=None,
    ) as subdag:
        task1 = PythonOperator(
            task_id='process_task1',
            python_callable=process_data,
            provide_context=True,
        )
        task2 = PythonOperator(
            task_id='process_task2',
            python_callable=process_data,
            provide_context=True,
        )

        task1 >> task2

    return subdag


# Define the main DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='complex_data_processing_dag',
    default_args=default_args,
    description='A complex DAG with branching, retries, and conditional logic.',
    schedule_interval=None,
) as dag:
    start_task = DummyOperator(
        task_id='start',
    )

    generate_task = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
        provide_context=True,
        retries=3,  # retry if fails
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_decision,
        provide_context=True,
    )

    task_process_data = PythonOperator(
        task_id='task_process_data',
        python_callable=process_data,
        provide_context=True,
    )

    task_failure = PythonOperator(
        task_id='task_failure',
        python_callable=handle_failure,
        provide_context=True,
    )

    # SubDagOperator example
    subdag_processing_task = SubDagOperator(
        task_id='subdag_processing',
        subdag=subdag_processing('complex_data_processing_dag', 'complex_data_processing_dag.subdag_processing', default_args),
    )

    end_task = DummyOperator(
        task_id='end',
    )

    # Setting up the task dependencies
    start_task >> generate_task >> branch_task
    branch_task >> task_process_data >> subdag_processing_task >> end_task
    branch_task >> task_failure >> end_task
