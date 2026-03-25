import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the Python function
def list_files():
    path = "/home/patil/airflow_project/dags"
    files = os.listdir(path)
    print(f"Files in {path}: {files}")

# Define the DAG
with DAG(
    dag_id="whoami_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # Run only when triggered manually
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    task2 = BashOperator(
        task_id="print_message",
        bash_command="echo 'Airflow DAG with multiple tasks!'"
    )

    # ✅ Correct usage of PythonOperator
    task3 = PythonOperator(
        task_id="list_files",
        python_callable=list_files
    )

    task1 >> task2 >> task3
