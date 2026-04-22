from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.python import PythonOperator
import os

def list_files():
    print(os.listdir())

with DAG(
    dag_id="whoami_dag_1",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id="whoami",
        bash_command="echo $(whoami)"
    )

    task2 = BashOperator(
        task_id="date",
        bash_command="echo $(date)"
    )

    task3 = PythonOperator(
        task_id="print",
        python_callable=list_files   # ✅ correct
    )

    task1 >> task2 >> task3
