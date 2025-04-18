from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'dnc',
    'retries': 5,
    'retries_delay': timedelta(seconds=15)
}

def my_task_1():
    logging.info('MINHA PRIMEIRA TASK')

def my_task_2():
    logging.info('MINHA SEGUNDA TASK')

'''def my_task_1():
    print('MINHA PRIMEIRA TASK')

def my_task_2():
    print('MINHA SEGUNDA TASK')'''

with DAG(
    dag_id='my_first_dag.py',
    default_args=default_args,
    description='Minha primeira DAG',
    start_date=datetime(2025,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_1 = PythonOperator(
        task_id = 'task1',
        python_callable=my_task_1
    )

    task_2 = PythonOperator(
        task_id = 'task2',
        python_callable=my_task_2
    )

task_1 >> task_2  