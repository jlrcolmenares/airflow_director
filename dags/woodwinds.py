from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import time

def play_sound():
    print("Task starts.")
    time.sleep(120)
    print("Task completed after 2 minutes.")

dag = DAG(
    dag_id='play_woodwings',
    description='A musician read his part and play',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2024, 1, 1),
)

# Assuming the signal file is located at '/tmp/my_temp_file.txt'
wait_for_file = ExternalTaskSensor(
    task_id='wait_for_file',
    external_dag_id='dummy_file_generator',
    external_task_id='create_file',
    poke_interval=60,  # checks every 60 seconds
    timeout=600,  # gives up after 10 minutes
    dag=dag
)

task_operator = PythonOperator(
    task_id='play_woodwings',
    python_callable=play_sound,
    dag=dag
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag
)

(
    wait_for_file
    >> task_operator
    >> end_dag
)
