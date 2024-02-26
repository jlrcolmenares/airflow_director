from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime


def decide_performance(**kwargs):
    # Simulating the decision based on a variable
    performance_quality = kwargs['ti'].xcom_pull(task_ids='check_performance_quality')
    if performance_quality == 'wonderful':
        return 'sing_wonderfully'
    else:
        return 'sing_wrong'

def sing_wonderfully():
    print("Musician sings a beautiful melody.")

def sing_wrong():
    print("Musician hits the wrong notes.")

def check_performance_quality(**kwargs):
    # Placeholder for logic that determines performance quality
    # This could be based on an external factor or random choice for the example
    import random
    quality = random.choice(['wonderful', 'wrong'])
    print(f"Performance quality is: {quality}")
    return quality



dag = DAG(
    dag_id='play_choir',
    description='A musician read his part and play or sing',
    schedule_interval="30 1 * * *",
    start_date=datetime(2024, 1, 1),
)

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

check_quality = PythonOperator(
    task_id='check_performance_quality',
    python_callable=check_performance_quality,
    provide_context=True,
    dag=dag
)

decision = BranchPythonOperator(
    task_id='decision',
    python_callable=decide_performance,
    provide_context=True,
    dag=dag,
)

wonderfully = PythonOperator(
    task_id='sing_wonderfully',
    python_callable=sing_wonderfully,
    dag=dag
)

wrong = PythonOperator(
    task_id='sing_wrong',
    python_callable=sing_wrong,
    dag=dag
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag
)

# Defining the workflow
(
    start_dag
    >> check_quality
    >> decision
    >> [wonderfully, wrong]
    >> end_dag
)
