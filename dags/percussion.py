from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import time

def play_sound():
    print("Musician starts.")
    time.sleep(5)  # Sleep for 5 seconds to simulate playing
    print("Musician plays something wonderfully")
    print("Musician stops")

dag = DAG(
    dag_id='play_percussion',
    description='A musician reads his part and plays',
    schedule_interval="20 1 * * *",
    start_date=datetime(2024, 1, 1),
)

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

tune_instruments = BashOperator(
    task_id='tune_instruments',
    bash_command='echo "Tuning all instruments"',
    dag=dag
)

setup_stage = BashOperator(
    task_id='setup_stage',
    bash_command='echo "Setting up the stage"',
    dag=dag
)

play_percussion = PythonOperator(
    task_id='play_percussion',
    python_callable=play_sound,
    dag=dag
)

review_performance = BashOperator(
    task_id='review_performance',
    bash_command='echo "Reviewing performance quality"',
    dag=dag
)


end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag
)

# Defining the workflow
(
    start_dag
    >> [tune_instruments, setup_stage]
    >> play_percussion
    >> review_performance
    >> end_dag
)
