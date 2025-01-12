from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.pipeline import run_pipeline

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'attribution_pipeline',
    default_args=default_args,
    description='Run the Attribution Pipeline with Airflow',
    schedule_interval=None,  # Run manually or set a cron schedule
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['pipeline', 'airflow']
) as dag:

    # Task to execute the pipeline
    run_pipeline_task = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline,
        op_kwargs={
            'start_date': '{{ dag_run.conf.get("start_date") }}',
            'end_date': '{{ dag_run.conf.get("end_date") }}',
        },
    )

    run_pipeline_task
