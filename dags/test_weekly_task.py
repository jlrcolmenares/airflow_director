from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from plugins.chime_operator import ChimeOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import random

def randomly_decide_outcome(**kwargs):
    state = random.choice(['success', 'failure'])
    return state

def decide_chime_sound(outcome):
    if outcome == 'success':
        return 'success_sound'
    else:
        return 'failure_sound'

dag = DAG(
    dag_id='test_weekly_pipeline',
    description='A weekly pipeline',
    schedule_interval='0 8 * * 1', # Run every Monday at 8:00 AM
    start_date=datetime(2024, 1, 1),
)

start = DummyOperator(task_id='start', dag=dag)

randomly_decide = PythonOperator(
    task_id='randomly_decide_outcome',
    python_callable=randomly_decide_outcome,
    provide_context=True,
    dag=dag,
)

notify_chime = PythonOperator(
    task_id='notify_chime',
    python_callable=decide_chime_sound,
    op_kwargs={'outcome': '{{ task_instance.xcom_pull(task_ids="randomly_decide_outcome") }}'},
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >> randomly_decide >> notify_chime >> end