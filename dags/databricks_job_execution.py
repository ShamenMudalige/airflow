"""
## Databricks Job Execution DAG

This DAG demonstrates how to run a Databricks job using Airflow.
It uses the DatabricksRunNowOperator to trigger an existing Databricks job
and monitor its execution status.

Connection: databricks_test
Job ID: 114761195634627
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.sensors.databricks import DatabricksRunNowSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'databricks_job_execution',
    default_args=default_args,
    description='Execute Databricks job via Airflow',
    schedule_interval=timedelta(hours=1),  # Adjust schedule as needed
    catchup=False,
    tags=['databricks', 'etl', 'data-processing'],
)

def log_job_start(**context):
    """Log the start of the Databricks job execution"""
    print(f"Starting Databricks job execution at {datetime.now()}")
    print(f"Connection ID: databricks_test")
    print(f"Job ID: 114761195634627")
    return "Job execution initiated"

def log_job_completion(**context):
    """Log the completion of the Databricks job"""
    # Get the run_id from the previous task
    run_id = context['task_instance'].xcom_pull(task_ids='run_databricks_job')
    print(f"Databricks job completed successfully at {datetime.now()}")
    print(f"Run ID: {run_id}")
    return "Job execution completed"

# Task 1: Log job start
start_task = PythonOperator(
    task_id='log_job_start',
    python_callable=log_job_start,
    dag=dag,
)

# Task 2: Run the Databricks job
run_databricks_job = DatabricksRunNowOperator(
    task_id='run_databricks_job',
    databricks_conn_id='databricks_test',
    job_id=114761195634627,
    # Optional: Pass parameters to the job
    notebook_params={
        'execution_date': '{{ ds }}',
        'dag_run_id': '{{ dag_run.run_id }}',
    },
    # Optional: Override job configuration
    jar_params=[],
    python_params=[],
    spark_submit_params=[],
    dag=dag,
)

# Task 3: Monitor job completion (optional - the RunNowOperator already waits for completion)
# This sensor can be used if you need additional monitoring or want to separate the triggering and monitoring
monitor_job = DatabricksRunNowSensor(
    task_id='monitor_databricks_job',
    databricks_conn_id='databricks_test',
    run_id="{{ task_instance.xcom_pull(task_ids='run_databricks_job') }}",
    timeout=3600,  # 1 hour timeout
    poke_interval=60,  # Check every 60 seconds
    dag=dag,
)

# Task 4: Log job completion
completion_task = PythonOperator(
    task_id='log_job_completion',
    python_callable=log_job_completion,
    dag=dag,
)

# Task 5: Cleanup or post-processing (placeholder)
cleanup_task = DummyOperator(
    task_id='cleanup_and_finalize',
    dag=dag,
)

# Define task dependencies
start_task >> run_databricks_job >> monitor_job >> completion_task >> cleanup_task

# Alternative simpler dependency chain (without separate monitoring)
# start_task >> run_databricks_job >> completion_task >> cleanup_task