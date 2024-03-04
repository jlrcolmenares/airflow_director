from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

dag = DAG(
    dag_id='test_scheduled_dag',
    description="This is a task that runs every week",
    schedule_interval="15 * * * *",
    start_date=datetime(2024, 1, 1),
)

start = DummyOperator(task_id='start')

folder_path = '/opt/airflow/tmp/data/my_temp_file.txt'

# Use the FileSensor to monitor the folder for file changes
file_sensor = FileSensor(
    task_id='file_sensor',
    filepath=f"{folder_path}/*",
    poke_interval=10,  # Set the interval between pokes
)


task1 = DummyOperator(task_id='task1')
task2 = DummyOperator(task_id='task2')


start >> file_sensor >> [task1, task2]
