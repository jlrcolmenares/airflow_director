from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import time

# Function to simulate playing sound
def play_sound(section):
    print(f"{section} starts.")
    time.sleep(60)  # Sleep for 1 minute to simulate a task
    print(f"{section} completed after 1 minute.")

# Main violinist playing the melody
def main_violinist():
    play_sound("Main violinist")

# Define the DAG
dag = DAG(
    dag_id='play_strings',
    description='An orchestra plays a piece with a main violinist leading',
    schedule_interval="5 1 * * *",
    start_date=datetime(2024, 1, 1),
)

# Start DAG
start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

# Main violinist task
main_violinist_task = PythonOperator(
    task_id='main_violinist_play',
    python_callable=main_violinist,
    dag=dag
)

# Section tasks
sections = ['violins', 'violas', 'cellos', 'bass']
section_tasks = []

for section in sections:
    task = PythonOperator(
        task_id=f'{section}_repeat_melody',
        python_callable=play_sound,
        op_kwargs={'section': f'{section.capitalize()}'},
        dag=dag
    )
    section_tasks.append(task)


end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag
)


# Defining the workflow
(
    start_dag
    >> main_violinist_task
    >> section_tasks
    >> end_dag
)
