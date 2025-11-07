"""
## Simple Databricks Job Execution DAG

This is a simplified DAG that follows Microsoft Azure documentation patterns
for running Databricks jobs with Apache Airflow.

Connection: databricks_test
Job ID: 114761195634627
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG following Microsoft documentation pattern
with DAG(
    'simple_databricks_job',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # Run daily
    default_args=default_args,
    description='Simple Databricks job execution via Airflow',
    catchup=False,
    tags=['databricks', 'simple']
) as dag:

    # Execute the Databricks job
    run_databricks_job = DatabricksRunNowOperator(
        task_id='run_databricks_job',
        databricks_conn_id='databricks_test',
        job_id=114761195634627,
        # Pass parameters to the job
        notebook_params={
            'execution_date': '{{ ds }}',
            'dag_run_id': '{{ dag_run.run_id }}',
        }
    )