from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def play_sound(**kwargs):
    # Accessing parameters passed to the Python callable
    execution_date = kwargs['execution_date']
    custom_message = kwargs['params'].get('custom_message', 'Default message')

    print("Musician starts.")
    print(f"Musician says: {custom_message} on {execution_date}")
    print("Musician stops")


dag = DAG(
    dag_id='play_brass',
    description='A musician read his part and play',
    schedule_interval="10 1 * * *",
    start_date=datetime(2024, 1, 1),
)

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

play_brass = PythonOperator(
    task_id='play_brass',
    python_callable=play_sound,
    provide_context=True,
    # Using Jinja to dynamically pass the execution date and custom params
    op_kwargs={'execution_date': '{{ ds }}'},
    params={'custom_message': 'Playing something wonderfully'},
    dag=dag
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag
)

# Defining the workflow
(
    start_dag
    >> play_brass
    >> end_dag

)