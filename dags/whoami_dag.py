from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
import os

with DAG(
    dag_id="whoami_dag",
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
        python_callable="print(os.listdir())"
    )

    task1 >> task2 >> task3
