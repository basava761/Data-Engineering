from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define Python callables
def start_task():
    print("Starting the workflow...")

def decide_branch():
    # Simple branching logic
    import random
    choice = random.choice(["task_a", "task_b"])
    print(f"Branching to: {choice}")
    return choice

def task_a_func():
    print("Task A executed successfully!")

def task_b_func():
    print("Task B executed successfully!")

def final_task():
    print("Final task executed after branch!")

# DAG definition
with DAG(
    dag_id="complex_multi_task_dag",
    start_date=datetime(2026, 3, 23),
    schedule=None,  # manual trigger
    catchup=False,
    tags=["example", "complex"],
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=start_task,
    )

    branch = BranchPythonOperator(
        task_id="branching",
        python_callable=decide_branch,
    )

    task_a = PythonOperator(
        task_id="task_a",
        python_callable=task_a_func,
    )

    task_b = PythonOperator(
        task_id="task_b",
        python_callable=task_b_func,
    )

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo 'Running a Bash command!'",
    )

    final = PythonOperator(
        task_id="final_task",
        python_callable=final_task,
    )

    # Define dependencies
    start >> branch
    branch >> [task_a, task_b]
    [task_a, task_b] >> bash_task >> final
