from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id='dummy_file_generator',
    description='Create a file in /opt/airflow/tmp/data/',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

create_delete_file = BashOperator(
    task_id='create_file',
    bash_command="""
    FILE_PATH="/opt/airflow/tmp/data/my_temp_file.txt"
    if [ -f "$FILE_PATH" ]; then
        echo "File $FILE_PATH exists. Deleting..."
        rm "$FILE_PATH"
    else
        echo "File $FILE_PATH does not exist. Creating..."
        touch "$FILE_PATH"
    fi
    """,
    dag=dag,
)

start_dag >> create_delete_file