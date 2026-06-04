from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    run_pyspark = BashOperator(
        task_id="run_pyspark_job",
        bash_command="python /opt/airflow/pyspark_jobs/preprocess.py"
    )

    run_pyspark