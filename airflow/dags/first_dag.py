from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def say_hello():
    print("Hello from my first Airflow DAG!")


with DAG(
    dag_id="first_financial_dag",
    start_date=datetime(2026, 8, 18),
    schedule=None,
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=say_hello,
    )