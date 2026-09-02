from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def run_spark_pipeline():
    import subprocess

    subprocess.run(
        [
            "python",
            "/opt/airflow/part2_cloud_spark/src/main.py"
        ],
        check=True
    )


with DAG(
    dag_id="financial_etl_pipeline",
    start_date=datetime(2026, 8, 26),
    schedule=None,
    catchup=False,

    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },

    tags=["spark", "etl", "financial"],
) as dag:

    run_pipeline = PythonOperator(
        task_id="run_spark_pipeline",
        python_callable=run_spark_pipeline,
    )





# from datetime import datetime, timedelta
# import pendulum

# from airflow import DAG
# from airflow.providers.standard.operators.python import PythonOperator


# IST = pendulum.timezone("Asia/Kolkata")


# def run_spark_pipeline():
#     import subprocess

#     subprocess.run(
#         [
#             "python",
#             "/opt/airflow/part2_cloud_spark/src/main.py"
#         ],
#         check=True
#     )


# with DAG(
#     dag_id="financial_etl_pipeline",
#     start_date=datetime(2026, 9, 1, tzinfo=IST),
#     schedule="10 23 * * *",
#     catchup=False,
#     default_args={
#         "retries": 2,
#         "retry_delay": timedelta(minutes=5),
#     },

#     tags=["spark", "etl", "financial"],
# ) as dag:

#     run_pipeline = PythonOperator(
#         task_id="run_spark_pipeline",
#         python_callable=run_spark_pipeline,
#     )