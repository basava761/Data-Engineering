from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # modern replacement for schedule_interval
    catchup=False,
) as dag:

    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello World from Airflow!'"
    )

