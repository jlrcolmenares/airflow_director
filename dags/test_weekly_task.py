from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import random
import chime

chime.theme('mario')


def randomly_decide_outcome():
    state = random.choice(['success', 'failure'])
    return state

def decide_chime_sound(state):
        if state == 'success':
            chime.success()
        elif state == 'failure':
            chime.failure()

dag = DAG(
    dag_id='test_weekly_pipeline',
    description='A weekly pipeline',
    schedule_interval='0 8 * * 1', # Run every Monday at 8:00 AM
    start_date=datetime(2024, 1, 1),
)

start_task = DummyOperator(task_id='start')

randomly_decide = PythonOperator(
    task_id='randomly_decide_outcome',
    python_callable=randomly_decide_outcome,
    provide_context=True,
    dag=dag,
)

notify_chime = PythonOperator(
    task_id='notify_chime',
    python_callable=decide_chime_sound,
    op_kwargs={'state': '{{ task_instance.xcom_pull(task_ids="randomly_decide_outcome") }}'},
    dag=dag,
)

end_task = DummyOperator(task_id='end')

# workflow
(
    start_task
    >> randomly_decide
    >> notify_chime
    >> end_task
)