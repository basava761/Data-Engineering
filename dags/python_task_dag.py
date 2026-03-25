from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a simple Python function
def greet(name: str):
    print(f"Hello, {name}! Airflow is running your Python task.")

# Define the DAG
with DAG(
    dag_id="python_task_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # Run only when triggered manually
    catchup=False,
) as dag:

    # Create a task using PythonOperator
    task = PythonOperator(
        task_id="greet_task",
        python_callable=greet,
        op_args=["Patil"],   # Pass arguments to the function
    )

